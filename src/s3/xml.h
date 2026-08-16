// L2: small XML generator and parser (S3 request/response structures are shallow and fixed-shape; no XML library dependency)
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace lights3::s3 {

std::string xml_escape(const std::string& s);

// ---------- Parsing (docs/s3-protocol.md §4: only shallow structures such as CompleteMultipartUpload / DeleteObjects) ----------
// Supports: nested elements, text, entities (lt gt amp quot apos #dd #xhh), comments, XML declaration, CDATA.
// Attributes are skipped (S3 request XML only carries xmlns). Malformed input or exceeding max_size throws S3Error{MalformedXML}.

struct XmlNode {
    std::string name;
    std::string text;  // direct text (concatenated, leading/trailing whitespace trimmed)
    std::vector<XmlNode> children;

    const XmlNode* find(std::string_view child_name) const;   // first child with the given name
    std::string get(std::string_view child_name) const;       // child node text, "" if absent
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
