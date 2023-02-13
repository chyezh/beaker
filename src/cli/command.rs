use super::{Error, Result};
use crate::cmd::{
    Command, Del, Get, Ping, Set, DEL_COMMAND_NAME, GET_COMMAND_NAME, PING_COMMAND_NAME,
    SET_COMMAND_NAME,
};
use bytes::Bytes;
use nom::{
    branch::alt,
    bytes::complete::{is_not, tag_no_case},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, value, verify},
    multi::{fold_many0, many0},
    sequence::{delimited, preceded},
    IResult,
};

pub fn parse(input: &str) -> Result<Command> {
    // Dispart a command into cmd and arguments
    let (input, cmd) = parse_cmd_name_with_space(input)?;
    let (input, args) = parse_arguments(input)?;
    if !input.is_empty() {
        Err(Error::ParsingIncomplete)?;
    }

    match cmd.to_uppercase().as_str() {
        GET_COMMAND_NAME => get(args),
        PING_COMMAND_NAME => ping(args),
        SET_COMMAND_NAME => set(args),
        DEL_COMMAND_NAME => del(args),
        _ => unreachable!(),
    }
}

/// Create a get operation from given args
fn get(mut args: Vec<String>) -> Result<Command> {
    if args.len() != 1 {
        Err(Error::InvalidArgsCount)?;
    }
    let mut args = args.drain(..);

    Ok(Get::new(Bytes::from(args.next().unwrap())).into())
}

/// Create a ping operation from given args
fn ping(mut args: Vec<String>) -> Result<Command> {
    if args.len() > 1 {
        Err(Error::InvalidArgsCount)?;
    }
    let mut args = args.drain(..);

    Ok(if let Some(msg) = args.next() {
        Ping::new(Some(Bytes::from(msg))).into()
    } else {
        Ping::new(None).into()
    })
}

/// Create a set operation from given args
fn set(mut args: Vec<String>) -> Result<Command> {
    if args.len() != 2 {
        Err(Error::InvalidArgsCount)?;
    }
    let mut args = args.drain(..);

    Ok(Set::new(
        Bytes::from(args.next().unwrap()),
        Bytes::from(args.next().unwrap()),
    )
    .into())
}

/// Create a del operation from given args
fn del(mut args: Vec<String>) -> Result<Command> {
    if args.len() != 1 {
        Err(Error::InvalidArgsCount)?;
    }
    let mut args = args.drain(..);

    Ok(Del::new(Bytes::from(args.next().unwrap())).into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringFragment<'a> {
    Literal(&'a str),
    EscapedChar(char),
}

/// Parse command name from given string.
/// Ignore prefix whitespace character.
fn parse_cmd_name_with_space(input: &str) -> IResult<&str, &str> {
    delimited(multispace0, parse_cmd_name, multispace1)(input)
}

/// Parse command name from given string.
fn parse_cmd_name(input: &str) -> IResult<&str, &str> {
    alt((
        tag_no_case(GET_COMMAND_NAME),
        tag_no_case(SET_COMMAND_NAME),
        tag_no_case(DEL_COMMAND_NAME),
        tag_no_case(PING_COMMAND_NAME),
    ))(input)
}

/// Parse argument list
fn parse_arguments(input: &str) -> IResult<&str, Vec<String>> {
    many0(parse_next_argument)(input)
}

/// Parse a command from cli standard input
fn parse_next_argument(input: &str) -> IResult<&str, String> {
    delimited(
        multispace0,
        alt((parse_literal_string, parse_escaped_string)),
        multispace0,
    )(input)
}

/// Parse a string without space
pub fn parse_literal_string(input: &str) -> IResult<&str, String> {
    let (input, s) = verify(is_not("\t\r\n\\\"\' "), |s: &str| !s.is_empty())(input)?;
    Ok((input, String::from(s)))
}

/// Parse escaped string
fn parse_escaped_string(input: &str) -> IResult<&str, String> {
    alt((
        build_double_quoted_unescaped_string,
        build_single_quoted_unescaped_string,
    ))(input)
}

/// Build a unescaped string from given escaped double quoted string.
/// Allocation of new string was needed.
fn build_double_quoted_unescaped_string(input: &str) -> IResult<&str, String> {
    delimited(
        char('"'),
        fold_many0(
            parse_double_quoted_escaped_string_fragment,
            String::new,
            |mut unescaped, fragment| {
                match fragment {
                    StringFragment::Literal(s) => unescaped.push_str(s),
                    StringFragment::EscapedChar(c) => unescaped.push(c),
                }
                unescaped
            },
        ),
        char('"'),
    )(input)
}

/// Parse escaped string fragment in double quoted string
fn parse_double_quoted_escaped_string_fragment(input: &str) -> IResult<&str, StringFragment> {
    alt((
        map(parse_double_quoted_literal, StringFragment::Literal),
        map(
            parse_double_quoted_escaped_char,
            StringFragment::EscapedChar,
        ),
    ))(input)
}

/// Parse a non-empty string without \ or "
fn parse_double_quoted_literal(input: &str) -> IResult<&str, &str> {
    verify(is_not("\"\\"), |s: &str| !s.is_empty())(input)
}

/// Unescape '\' escaped char in double quoted string
fn parse_double_quoted_escaped_char(input: &str) -> IResult<&str, char> {
    preceded(char('\\'), transform_double_quoted_escaped_char)(input)
}

/// Transform all escaped char and convert into unescaped char in double quoted string
fn transform_double_quoted_escaped_char(input: &str) -> IResult<&str, char> {
    alt((
        value('"', char('"')),
        value('\n', char('n')),
        value('\r', char('r')),
        value('\t', char('t')),
        value('\\', char('\\')),
    ))(input)
}

/// Build a unescaped string from given escaped single quoted string.
/// Allocation of new string was needed.
fn build_single_quoted_unescaped_string(input: &str) -> IResult<&str, String> {
    delimited(
        char('\''),
        fold_many0(
            parse_single_quoted_escaped_string_fragment,
            String::new,
            |mut unescaped, fragment| {
                match fragment {
                    StringFragment::Literal(s) => unescaped.push_str(s),
                    StringFragment::EscapedChar(c) => unescaped.push(c),
                }
                unescaped
            },
        ),
        char('\''),
    )(input)
}

/// Parse escaped string
fn parse_single_quoted_escaped_string_fragment(input: &str) -> IResult<&str, StringFragment> {
    alt((
        map(parse_single_quoted_literal, StringFragment::Literal),
        map(
            parse_single_quoted_escaped_char,
            StringFragment::EscapedChar,
        ),
    ))(input)
}

/// Parse a non-empty string without \ or '
fn parse_single_quoted_literal(input: &str) -> IResult<&str, &str> {
    verify(is_not("\'\\"), |s: &str| !s.is_empty())(input)
}

/// Unescape '\' escaped char in single quoted string
fn parse_single_quoted_escaped_char(input: &str) -> IResult<&str, char> {
    preceded(char('\\'), transform_single_quoted_escaped_char)(input)
}

/// Transform all escaped char and convert into unescaped char in single quoted string
fn transform_single_quoted_escaped_char(input: &str) -> IResult<&str, char> {
    alt((value('\'', char('\'')), value('\\', char('\\'))))(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        assert_eq!(
            parse("get 123").unwrap(),
            Get::new(Bytes::from("123")).into()
        );
        assert_eq!(
            parse("get \"123\"").unwrap(),
            Get::new(Bytes::from("123")).into()
        );
        assert_eq!(
            parse("get     \"123\"     ").unwrap(),
            Get::new(Bytes::from("123")).into()
        );
    }

    #[test]
    fn test_parse_cmd_name() {
        assert_eq!(parse_cmd_name("get"), Ok(("", "get")));
        assert_eq!(parse_cmd_name("del"), Ok(("", "del")));
        assert_eq!(parse_cmd_name("ping"), Ok(("", "ping")));
        assert_eq!(parse_cmd_name("set"), Ok(("", "set")));

        assert_eq!(parse_cmd_name("GET"), Ok(("", "GET")));
        assert_eq!(parse_cmd_name("DEL"), Ok(("", "DEL")));
        assert_eq!(parse_cmd_name("PING"), Ok(("", "PING")));
        assert_eq!(parse_cmd_name("SET"), Ok(("", "SET")));
    }

    #[test]
    fn test_parse_cmd_name_with_space() {
        assert_eq!(parse_cmd_name_with_space("    get "), Ok(("", "get")));
        assert_eq!(parse_cmd_name_with_space("    get     "), Ok(("", "get")));
        assert_eq!(parse_cmd_name_with_space("    get \t    "), Ok(("", "get")));
        assert_eq!(
            parse_cmd_name_with_space(" \t   get \t    "),
            Ok(("", "get"))
        );
        assert_eq!(parse_cmd_name_with_space("get "), Ok(("", "get")));
        assert_eq!(parse_cmd_name_with_space("\tget\t"), Ok(("", "get")));
        assert_eq!(parse_cmd_name_with_space("    get\t"), Ok(("", "get")));

        assert!(parse_cmd_name_with_space("  get").is_err());
        assert!(parse_cmd_name_with_space("\tget").is_err());
    }

    #[test]
    fn test_parse_next_argument() {
        let case =
            "key1  \t key2  \"tab:\\t, newline:\\n, slash:\\\\, quote:\\\", literal: abc \"   key3 'quote:\\', slash:\\\\, literal: abc '   key4   ";
        let (case, argument) = parse_next_argument(case).unwrap();
        assert_eq!(argument, "key1");
        let (case, argument) = parse_next_argument(case).unwrap();
        assert_eq!(argument, "key2");
        let (case, argument) = parse_next_argument(case).unwrap();
        assert_eq!(
            argument,
            "tab:\t, newline:\n, slash:\\, quote:\", literal: abc "
        );
        let (case, argument) = parse_next_argument(case).unwrap();
        assert_eq!(argument, "key3");
        let (case, argument) = parse_next_argument(case).unwrap();
        assert_eq!(argument, "quote:', slash:\\, literal: abc ");
        let (case, argument) = parse_next_argument(case).unwrap();
        assert_eq!(argument, "key4");
        assert_eq!(case, "");
    }

    #[test]
    fn test_build_double_quoted_unescaped_string() {
        assert_eq!(
            build_double_quoted_unescaped_string("\"abc\""),
            Ok(("", String::from("abc")))
        );
        assert_eq!(
            build_double_quoted_unescaped_string(
                "\"tab:\\t, newline:\\n, slash:\\\\, quote:\\\", literal: abc \""
            ),
            Ok((
                "",
                String::from("tab:\t, newline:\n, slash:\\, quote:\", literal: abc ")
            ))
        );
    }

    #[test]
    fn test_build_single_quoted_unescaped_string() {
        assert_eq!(
            build_single_quoted_unescaped_string("''"),
            Ok(("", String::from("")))
        );
        assert_eq!(
            build_single_quoted_unescaped_string("'abc'"),
            Ok(("", String::from("abc")))
        );
        assert_eq!(
            build_single_quoted_unescaped_string("'quote:\\', slash:\\\\, literal: abc '"),
            Ok(("", String::from("quote:', slash:\\, literal: abc ")))
        );
    }
}
