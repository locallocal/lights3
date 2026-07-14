#include "s3/xml.h"

#include <cctype>

#include "s3/errors.h"

namespace lights3::s3 {

std::string xml_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        switch (c) {
            case '&': out += "&amp;"; break;
            case '<': out += "&lt;"; break;
            case '>': out += "&gt;"; break;
            case '"': out += "&quot;"; break;
            case '\'': out += "&apos;"; break;
            default: out.push_back(c);
        }
    }
    return out;
}

// ---------- 解析 ----------

namespace {

[[noreturn]] void bad(const std::string& why) {
    throw S3Error(S3ErrorCode::MalformedXML,
                  "The XML you provided was not well-formed: " + why);
}

class Parser {
public:
    explicit Parser(std::string_view s) : s_(s) {}

    XmlNode parse_document() {
        skip_misc();
        if (eof()) bad("empty document");
        XmlNode root = parse_element();
        skip_misc();
        if (!eof()) bad("trailing content after root element");
        return root;
    }

private:
    bool eof() const { return pos_ >= s_.size(); }
    char peek() const { return s_[pos_]; }
    bool starts_with(std::string_view p) const { return s_.substr(pos_, p.size()) == p; }

    void skip_ws() {
        while (!eof() && std::isspace(static_cast<unsigned char>(peek()))) ++pos_;
    }

    void skip_until(std::string_view end, const char* what) {
        auto at = s_.find(end, pos_);
        if (at == std::string_view::npos) bad(std::string("unterminated ") + what);
        pos_ = at + end.size();
    }

    // 根元素前后与元素间的杂项：空白、声明、注释、DOCTYPE（跳过，不展开实体）
    void skip_misc() {
        for (;;) {
            skip_ws();
            if (starts_with("<?")) skip_until("?>", "processing instruction");
            else if (starts_with("<!--")) skip_until("-->", "comment");
            else if (starts_with("<!DOCTYPE")) skip_until(">", "DOCTYPE");
            else return;
        }
    }

    std::string parse_name() {
        size_t start = pos_;
        while (!eof()) {
            char c = peek();
            if (std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-' ||
                c == '.' || c == ':')
                ++pos_;
            else
                break;
        }
        if (pos_ == start) bad("expected element name");
        return std::string(s_.substr(start, pos_ - start));
    }

    // 已定位到 '<'；解析整个元素（含子元素与文本）
    XmlNode parse_element() {
        if (++depth_ > 32) bad("nesting too deep");
        ++pos_;  // '<'
        XmlNode node;
        node.name = parse_name();

        // 属性统一跳过（S3 请求 XML 只有 xmlns）
        for (;;) {
            skip_ws();
            if (eof()) bad("unterminated start tag");
            if (peek() == '>') {
                ++pos_;
                break;
            }
            if (starts_with("/>")) {
                pos_ += 2;
                --depth_;
                return node;  // 自闭合
            }
            parse_attribute();
        }

        // 内容
        std::string text;
        for (;;) {
            if (eof()) bad("unterminated element <" + node.name + ">");
            if (starts_with("</")) {
                pos_ += 2;
                std::string close = parse_name();
                if (close != node.name)
                    bad("mismatched close tag </" + close + "> for <" + node.name + ">");
                skip_ws();
                if (eof() || peek() != '>') bad("malformed close tag");
                ++pos_;
                break;
            }
            if (starts_with("<!--")) {
                skip_until("-->", "comment");
            } else if (starts_with("<![CDATA[")) {
                size_t at = s_.find("]]>", pos_ + 9);
                if (at == std::string_view::npos) bad("unterminated CDATA");
                text.append(s_.substr(pos_ + 9, at - pos_ - 9));
                pos_ = at + 3;
            } else if (peek() == '<') {
                node.children.push_back(parse_element());
            } else {
                parse_text(text);
            }
        }
        node.text = trim(text);
        --depth_;
        return node;
    }

    void parse_attribute() {
        parse_name();
        skip_ws();
        if (eof() || peek() != '=') bad("malformed attribute");
        ++pos_;
        skip_ws();
        if (eof() || (peek() != '"' && peek() != '\'')) bad("attribute value must be quoted");
        char quote = peek();
        ++pos_;
        auto at = s_.find(quote, pos_);
        if (at == std::string_view::npos) bad("unterminated attribute value");
        pos_ = at + 1;
    }

    void parse_text(std::string& out) {
        while (!eof() && peek() != '<') {
            if (peek() == '&') {
                parse_entity(out);
            } else {
                out.push_back(peek());
                ++pos_;
            }
        }
    }

    void parse_entity(std::string& out) {
        auto semi = s_.find(';', pos_);
        if (semi == std::string_view::npos || semi - pos_ > 8) bad("malformed entity");
        std::string_view e = s_.substr(pos_ + 1, semi - pos_ - 1);
        pos_ = semi + 1;
        if (e == "lt") out.push_back('<');
        else if (e == "gt") out.push_back('>');
        else if (e == "amp") out.push_back('&');
        else if (e == "quot") out.push_back('"');
        else if (e == "apos") out.push_back('\'');
        else if (!e.empty() && e[0] == '#') {
            long code = 0;
            try {
                code = (e.size() > 1 && (e[1] == 'x' || e[1] == 'X'))
                           ? std::stol(std::string(e.substr(2)), nullptr, 16)
                           : std::stol(std::string(e.substr(1)));
            } catch (...) {
                bad("malformed character reference");
            }
            if (code <= 0 || code > 0x10FFFF) bad("character reference out of range");
            append_utf8(out, static_cast<uint32_t>(code));
        } else {
            bad("unknown entity &" + std::string(e) + ";");
        }
    }

    static void append_utf8(std::string& out, uint32_t cp) {
        if (cp < 0x80) {
            out.push_back(static_cast<char>(cp));
        } else if (cp < 0x800) {
            out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else if (cp < 0x10000) {
            out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else {
            out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        }
    }

    static std::string trim(const std::string& s) {
        size_t b = s.find_first_not_of(" \t\r\n");
        if (b == std::string::npos) return "";
        size_t e = s.find_last_not_of(" \t\r\n");
        return s.substr(b, e - b + 1);
    }

    std::string_view s_;
    size_t pos_ = 0;
    int depth_ = 0;
};

}  // namespace

const XmlNode* XmlNode::find(std::string_view child_name) const {
    for (auto& c : children)
        if (c.name == child_name) return &c;
    return nullptr;
}

std::string XmlNode::get(std::string_view child_name) const {
    auto* c = find(child_name);
    return c ? c->text : "";
}

XmlNode xml_parse(std::string_view input, size_t max_size) {
    if (input.size() > max_size)
        throw S3Error(S3ErrorCode::MalformedXML, "Request XML exceeds the size limit.");
    return Parser(input).parse_document();
}

}  // namespace lights3::s3
