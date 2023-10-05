use rgb::RGB;

pub const WHITE_RGB: RGB<u8> = RGB::new(255, 255, 255);

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

pub fn fix_background_colour_rgb_feed(feed_id: &String, background: RGB<u8>) -> RGB<u8> {
    match feed_id.as_str() {
        "f-9q5b-longbeachtransit" => {
            if (background == WHITE_RGB) {
                return RGB::new(128, 31, 58);
            } else {
                return fix_background_colour_rgb(background);
            }
        }
        "f-9q5-metro~losangeles" => {
            if (background == WHITE_RGB) {
                return RGB::new(225, 103, 16);
            } else {
                return fix_background_colour_rgb(background);
            }
        }
        _ => fix_background_colour_rgb(background),
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

pub fn fix_foreground_colour_rgb_feed(
    feed_id: &String,
    background: RGB<u8>,
    foreground: RGB<u8>,
) -> RGB<u8> {
    match feed_id.as_str() {
        "f-9q5b-longbeachtransit" => WHITE_RGB,
        _ => fix_foreground_colour_rgb(background, foreground),
    }
}
