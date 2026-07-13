// L2: 小型 XML 生成器（S3 响应结构简单固定，不引入 XML 库）
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace lights3::s3 {

std::string xml_escape(const std::string& s);

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
    void element(const std::string& tag, uint64_t n) { element(tag, std::to_string(n)); }
    const std::string& str() const { return out_; }

private:
    std::string out_;
    std::vector<std::string> stack_;
};

}  // namespace lights3::s3
