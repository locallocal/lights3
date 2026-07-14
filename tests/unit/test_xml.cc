// XML 解析器（docs/05 §4：浅结构请求 XML）
#include "s3/errors.h"
#include "s3/xml.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::s3;

TEST(xml_parse_delete_objects_shape) {
    auto root = xml_parse(R"(<?xml version="1.0" encoding="UTF-8"?>
<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Object><Key>dir/a.txt</Key></Object>
  <Object><Key>b &amp; c.bin</Key></Object>
  <Quiet>true</Quiet>
</Delete>)");
    CHECK_EQ(root.name, "Delete");
    CHECK_EQ(root.get("Quiet"), "true");
    std::vector<std::string> keys;
    for (auto& c : root.children)
        if (c.name == "Object") keys.push_back(c.get("Key"));
    CHECK_EQ(keys.size(), size_t(2));
    CHECK_EQ(keys[0], "dir/a.txt");
    CHECK_EQ(keys[1], "b & c.bin");  // 实体解码
}

TEST(xml_parse_complete_multipart_shape) {
    auto root = xml_parse(
        "<CompleteMultipartUpload>"
        "<Part><PartNumber>1</PartNumber><ETag>\"abc\"</ETag></Part>"
        "<Part><PartNumber>2</PartNumber><ETag>def</ETag></Part>"
        "</CompleteMultipartUpload>");
    CHECK_EQ(root.children.size(), size_t(2));
    CHECK_EQ(root.children[0].get("PartNumber"), "1");
    CHECK_EQ(root.children[0].get("ETag"), "\"abc\"");
    CHECK_EQ(root.children[1].get("ETag"), "def");
}

TEST(xml_parse_entities_cdata_comments) {
    auto root = xml_parse(
        "<R><!-- comment --><A>&lt;x&gt; &#65;&#x42;</A><B><![CDATA[raw <>&]]></B>"
        "<C/><D attr=\"ignored\">t</D></R>");
    CHECK_EQ(root.get("A"), "<x> AB");
    CHECK_EQ(root.get("B"), "raw <>&");
    CHECK(root.find("C") != nullptr);
    CHECK_EQ(root.get("D"), "t");
}

TEST(xml_parse_malformed) {
    CHECK_THROWS_S3(xml_parse("<A><B></A></B>"), S3ErrorCode::MalformedXML);
    CHECK_THROWS_S3(xml_parse("<A>unterminated"), S3ErrorCode::MalformedXML);
    CHECK_THROWS_S3(xml_parse("<A/>trailing"), S3ErrorCode::MalformedXML);
    CHECK_THROWS_S3(xml_parse("plain text"), S3ErrorCode::MalformedXML);
    CHECK_THROWS_S3(xml_parse("<A>&bogus;</A>"), S3ErrorCode::MalformedXML);
    CHECK_THROWS_S3(xml_parse("<A>x</A>", /*max_size=*/4), S3ErrorCode::MalformedXML);
}

TEST(xml_escape_roundtrip) {
    XmlWriter w;
    w.open("R");
    w.element("K", "a<b>&\"'");
    w.close();
    auto root = xml_parse(w.str());
    CHECK_EQ(root.get("K"), "a<b>&\"'");
}
