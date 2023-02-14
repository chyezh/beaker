use std::{ops::RangeInclusive, path::PathBuf, str::FromStr};

const PORT_RANGE: RangeInclusive<u16> = 1..=65535;

pub fn port_in_range(s: &str) -> Result<u16, String> {
    let port = s
        .parse()
        .map_err(|_| format!("`{}` isn't a port number", s))?;

    if PORT_RANGE.contains(&port) {
        Ok(port)
    } else {
        Err(format!(
            "port must in range {}-{}",
            PORT_RANGE.start(),
            PORT_RANGE.end()
        ))
    }
}

pub fn valid_hostname(s: &str) -> Result<String, String> {
    if !hostname_validator::is_valid(s) {
        Err(format!("`{}` isn't a valid hostname", s))?;
    }
    Ok(String::from(s))
}

pub fn valid_path(s: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from_str(s).map_err(|e| format!("`{}` isn't a valid path: {}", s, e))?;
    Ok(path)
}
