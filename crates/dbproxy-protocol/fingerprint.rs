pub fn normalize_line_endings(schema: &[u8]) -> Vec<u8> {
    let mut normalized = Vec::with_capacity(schema.len());
    let mut index = 0;
    while index < schema.len() {
        if schema[index] == b'\r' {
            normalized.push(b'\n');
            if schema.get(index + 1) == Some(&b'\n') {
                index += 1;
            }
        } else {
            normalized.push(schema[index]);
        }
        index += 1;
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_lf_crlf_mixed_and_lone_cr_equally() {
        let expected = b"syntax = \"proto3\";\nmessage Ping {}\n";
        assert_eq!(normalize_line_endings(expected), expected);
        assert_eq!(
            normalize_line_endings(b"syntax = \"proto3\";\r\nmessage Ping {}\r\n"),
            expected
        );
        assert_eq!(
            normalize_line_endings(b"syntax = \"proto3\";\r\nmessage Ping {}\n"),
            expected
        );
        assert_eq!(
            normalize_line_endings(b"syntax = \"proto3\";\rmessage Ping {}\r"),
            expected
        );
    }
}
