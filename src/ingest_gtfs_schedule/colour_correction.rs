use rgb::RGB;

pub fn fix_background_colour(input: &str) -> &str {
    if input == "ffffff" || input == "000000" {
        "0ea5e9"
    } else {
        input
    }
}

pub fn fix_background_colour_rgb(background: RGB<u8>) -> RGB<u8> {
    if background == RGB::new(255, 255, 255) || background == RGB::new(0, 0, 0) {
        RGB::new(14, 165, 233)
    } else {
        background.clone()
    }
}

pub fn fix_foreground_colour<'a>(background: &'a str, foreground: &'a str) -> &'a str {
    if background == foreground {
        "000000"
    } else {
        foreground
    }
}

pub fn fix_foreground_colour_rgb(background: RGB<u8>, foreground: RGB<u8>) -> RGB<u8> {
    if background == foreground {
        RGB::new(0, 0, 0)
    } else {
        foreground.clone()
    }
}
