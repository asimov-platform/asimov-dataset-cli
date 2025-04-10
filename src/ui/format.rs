// This is free and unencumbered software released into the public domain.

/// ```
/// # use asimov_dataset_cli::ui::format_bytes;
/// assert_eq!("256 B", format_bytes(256).as_str());
/// assert_eq!("999 B", format_bytes(999).as_str());
/// assert_eq!("1.0 KB", format_bytes(1024).as_str());
/// assert_eq!("4.1 KB", format_bytes(1<<12).as_str());
/// assert_eq!("524.3 KB", format_bytes(1<<19).as_str());
/// assert_eq!("2.1 MB", format_bytes((1<<21)+1).as_str());
/// assert_eq!("2.1 MB", format_bytes((1<<21)+500).as_str());
/// assert_eq!("1.1 GB", format_bytes((1<<30)).as_str());
/// assert_eq!("1.0 GB", format_bytes(1000*1000*1000).as_str());
/// assert_eq!("4.5 PB", format_bytes(1<<52).as_str());
/// ```
pub fn format_bytes(n: usize) -> String {
    const KB: usize = 1_000;
    const MB: usize = KB * 1000;
    const GB: usize = MB * 1000;
    const TB: usize = GB * 1000;
    const PB: usize = TB * 1000;

    match n {
        ..KB => format!("{n} B"),
        KB..MB => format!("{:.1} KB", (n as f64 / KB as f64)),
        MB..GB => format!("{:.1} MB", (n as f64 / MB as f64)),
        GB..TB => format!("{:.1} GB", (n as f64 / GB as f64)),
        TB..PB => format!("{:.1} TB", (n as f64 / TB as f64)),
        PB.. => format!("{:.1} PB", (n as f64 / PB as f64)),
    }
}

/// ```
/// # use asimov_dataset_cli::ui::format_number;
/// assert_eq!("123", format_number(123).as_str());
/// assert_eq!("1_234", format_number(1234).as_str());
/// assert_eq!("123_456", format_number(123456).as_str());
/// assert_eq!("1_234_567", format_number(1234567).as_str());
/// ```
pub fn format_number(n: usize) -> String {
    let mut out = String::new();
    let digits = n.to_string();
    let len = digits.len();

    for (i, c) in digits.chars().enumerate() {
        out.push(c);
        // Add underscore after every 3rd digit from the right, except at the end
        if (len - i - 1) % 3 == 0 && i < len - 1 {
            out.push('_');
        }
    }

    out
}
