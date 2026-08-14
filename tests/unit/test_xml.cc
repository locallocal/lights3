// XML 解析器（docs/s3-protocol.md §4：浅结构请求 XML）
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

// 补充平面数字字符引用（docs/issues.md T12）：&#x1F600; 等 5-6 位十六进制引用
// 曾被长度护栏误拒成 MalformedXML
TEST(xml_parse_supplementary_plane_char_refs) {
    auto root = xml_parse("<R><A>&#x1F600;</A><B>&#x10FFFF;</B><C>&#1114111;</C></R>");
    CHECK_EQ(root.get("A"), "\xF0\x9F\x98\x80");  // U+1F600
    CHECK_EQ(root.get("B"), "\xF4\x8F\xBF\xBF");  // U+10FFFF
    CHECK_EQ(root.get("C"), "\xF4\x8F\xBF\xBF");  // 十进制同码点
    // 护栏仍在：离谱长度的引用照旧拒绝
    CHECK_THROWS_S3(xml_parse("<A>&#x000000000000000041;</A>"), S3ErrorCode::MalformedXML);
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
