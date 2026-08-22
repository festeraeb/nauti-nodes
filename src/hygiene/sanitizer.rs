//! Evidence sanitization for privacy boundaries, secret stripping, and path normalization.

use crate::index::compiler_memory::CompilerIncident;
use crate::index::symbol::CodeSymbol;

/// Rule-based secret and absolute path sanitizer.
#[derive(Debug, Clone, Default)]
pub struct EvidenceSanitizer;

impl EvidenceSanitizer {
    /// Creates a new EvidenceSanitizer.
    pub fn new() -> Self {
        Self
    }

    /// Sanitizes arbitrary text by redacting API keys, bearer tokens, private credentials, and absolute home directories.
    pub fn sanitize_text(&self, input: &str) -> String {
        let mut result = input.to_string();

        // 1. Redact Bearer tokens
        result = Self::regex_replace_bearer(&result);

        // 2. Redact OpenAI / Anthropic / GitHub / Google API keys
        result = Self::redact_known_key_patterns(&result);

        // 3. Normalize and redact home directories (e.g. /home/user or /Users/user)
        result = Self::redact_home_paths(&result);

        result
    }

    /// Sanitizes a code symbol doc comments, signatures, and qualified names.
    pub fn sanitize_symbol(&self, symbol: &CodeSymbol) -> CodeSymbol {
        let mut sanitized = symbol.clone();
        if let Some(doc) = &sanitized.doc_comment {
            sanitized.doc_comment = Some(self.sanitize_text(doc));
        }
        if let Some(sig) = &sanitized.signature {
            sanitized.signature = Some(self.sanitize_text(sig));
        }
        sanitized
    }

    /// Sanitizes a compiler incident diagnostic output.
    pub fn sanitize_incident(&self, incident: &CompilerIncident) -> CompilerIncident {
        let mut sanitized = incident.clone();
        sanitized.diagnostic = self.sanitize_text(&incident.diagnostic);
        sanitized
    }

    fn regex_replace_bearer(text: &str) -> String {
        let mut output = String::with_capacity(text.len());
        let mut remaining = text;

        while let Some(pos) = remaining.to_lowercase().find("bearer ") {
            output.push_str(&remaining[..pos]);
            output.push_str("Bearer ");
            let rest = &remaining[pos + 7..];
            let token_end = rest
                .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == ',' || c == ';')
                .unwrap_or(rest.len());
            let token = &rest[..token_end];

            if token.len() >= 8 {
                output.push_str("[REDACTED_BEARER_TOKEN]");
            } else {
                output.push_str(token);
            }
            remaining = &rest[token_end..];
        }
        output.push_str(remaining);
        output
    }

    fn redact_known_key_patterns(text: &str) -> String {
        let patterns = [
            ("sk-", 20, "[REDACTED_OPENAI_KEY]"),
            ("ghp_", 20, "[REDACTED_GITHUB_TOKEN]"),
            ("gho_", 20, "[REDACTED_GITHUB_TOKEN]"),
            ("AIza", 25, "[REDACTED_GOOGLE_API_KEY]"),
            ("xoxb-", 20, "[REDACTED_SLACK_TOKEN]"),
            ("xoxp-", 20, "[REDACTED_SLACK_TOKEN]"),
        ];

        let mut current = text.to_string();
        for (prefix, min_len, placeholder) in patterns {
            let mut next = String::new();
            let mut rem = current.as_str();

            while let Some(pos) = rem.find(prefix) {
                next.push_str(&rem[..pos]);
                let rest = &rem[pos..];
                let key_end = rest
                    .find(|c: char| {
                        !c.is_ascii_alphanumeric() && c != '-' && c != '_' && c != '.'
                    })
                    .unwrap_or(rest.len());
                let key_candidate = &rest[..key_end];

                if key_candidate.len() >= min_len {
                    next.push_str(placeholder);
                } else {
                    next.push_str(key_candidate);
                }
                rem = &rest[key_end..];
            }
            next.push_str(rem);
            current = next;
        }

        current
    }

    fn redact_home_paths(text: &str) -> String {
        let home_prefixes = ["/home/", "/Users/"];
        let mut current = text.to_string();

        for prefix in home_prefixes {
            let mut next = String::new();
            let mut rem = current.as_str();

            while let Some(pos) = rem.find(prefix) {
                next.push_str(&rem[..pos]);
                let rest = &rem[pos + prefix.len()..];
                // Find next slash for username
                if let Some(user_slash) = rest.find('/') {
                    next.push_str("~/");
                    rem = &rest[user_slash + 1..];
                } else {
                    next.push_str("~/");
                    rem = "";
                }
            }
            next.push_str(rem);
            current = next;
        }

        current
    }
}
