use std::fmt::Display;

/// Key to find FieldName tuple.
///
/// Represented in a string either like "(prefix).(attr)" or "(attr)".
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub struct FieldPointer {
    prefix: Option<String>,
    attr: String,
}

impl FieldPointer {
    /// Optional prefix part
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// Attribute part
    pub fn attr(&self) -> &str {
        &self.attr
    }
}

impl Display for FieldPointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = if let Some(p) = self.prefix() {
            format!("{}.", p)
        } else {
            "".to_string()
        };
        write!(f, "{}{}", prefix, self.attr())
    }
}

impl From<&str> for FieldPointer {
    fn from(s: &str) -> Self {
        let parts: Vec<&str> = s.split('.').collect();

        debug_assert!(!parts.is_empty());
        assert!(parts.len() <= 2, "too many dots (.) !");

        parts
            .iter()
            .for_each(|part| assert!(!part.is_empty(), "prefix nor attr must not be empty string"));

        let first = parts
            .get(0)
            .expect("must have at least 1 part")
            .trim()
            .to_string();
        let second = parts.get(1).map(|s| s.trim().to_string());

        if let Some(second) = second {
            Self::new(Some(first), second)
        } else {
            Self::new(None, first)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_success() {
        let from_to_data: Vec<(&str, &str)> = vec![
            ("c", "c"),
            ("  c ", "c"),
            ("t.c", "t.c"),
            ("   t  .  c    ", "t.c"),
        ];

        for (from, to) in from_to_data {
            assert_eq!(FieldPointer::from(from).to_string(), to);
        }
    }

    #[test]
    #[should_panic]
    fn test_from_panic1() {
        FieldPointer::from("");
    }

    #[test]
    #[should_panic]
    fn test_from_panic2() {
        FieldPointer::from(".c");
    }

    #[test]
    #[should_panic]
    fn test_from_panic3() {
        FieldPointer::from("t.");
    }

    #[test]
    #[should_panic]
    fn test_from_panic4() {
        FieldPointer::from("a.b.c");
    }
}
