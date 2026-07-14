// L2: 小型 XML 生成器与解析器（S3 请求/响应结构浅且模式固定，不引入 XML 库）
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace lights3::s3 {

std::string xml_escape(const std::string& s);

// ---------- 解析（docs/05 §4：仅 CompleteMultipartUpload / DeleteObjects 等浅结构）----------
// 支持：元素嵌套、文本、实体（lt gt amp quot apos #dd #xhh）、注释、XML 声明、CDATA。
// 属性被跳过（S3 请求 XML 只有 xmlns）。格式错误或超过 max_size 抛 S3Error{MalformedXML}。

struct XmlNode {
    std::string name;
    std::string text;  // 直接文本（拼接、去首尾空白）
    std::vector<XmlNode> children;

    const XmlNode* find(std::string_view child_name) const;   // 首个同名子节点
    std::string get(std::string_view child_name) const;       // 子节点文本，缺省 ""
};

XmlNode xml_parse(std::string_view input, size_t max_size = 1024 * 1024);

class XmlWriter {
public:
    XmlWriter() { out_ = R"(<?xml version="1.0" encoding="UTF-8"?>)"; }

    void open(const std::string& tag, const std::string& attrs = "") {
        out_ += "<" + tag + (attrs.empty() ? "" : " " + attrs) + ">";
        stack_.push_back(tag);
    }
    void close() {
        out_ += "</" + stack_.back() + ">";
        stack_.pop_back();
    }
    void element(const std::string& tag, const std::string& text) {
        out_ += "<" + tag + ">" + xml_escape(text) + "</" + tag + ">";
    }
    void text(const std::string& s) { out_ += xml_escape(s); }
    void element(const std::string& tag, uint64_t n) { element(tag, std::to_string(n)); }
    const std::string& str() const { return out_; }

private:
    std::string out_;
    std::vector<std::string> stack_;
};

}  // namespace lights3::s3
