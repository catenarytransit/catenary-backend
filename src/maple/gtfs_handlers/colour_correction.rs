use rgb::RGB;

pub const WHITE_RGB: RGB<u8> = RGB::new(255, 255, 255);

pub fn fix_background_colour_rgb(background: RGB<u8>) -> RGB<u8> {
    if background == RGB::new(255, 255, 255) || background == RGB::new(0, 0, 0) {
        RGB::new(14, 165, 233)
    } else {
        background
    }
}

pub fn fix_background_colour_rgb_feed_route(
    feed_id: &str,
    background: RGB<u8>,
    route: &gtfs_structures::Route,
) -> RGB<u8> {
    match feed_id {
        "f-bus~dft~gov~uk" => match &route.short_name {
            Some(short_name) => match short_name.as_str() {
                "Bakerloo" => RGB::new(166, 90, 42),
                "Central" => RGB::new(225, 37, 27),
                "Circle" => RGB::new(255, 205, 0),
                "District" => RGB::new(20, 121, 52),
                "Hammersmith & City" => RGB::new(236, 155, 173),
                "Jubilee" => RGB::new(123, 134, 140),
                "Metropolitan" => RGB::new(135, 15, 84),
                "Northern" => RGB::new(0, 0, 0),
                "Piccadilly" => RGB::new(0, 15, 159),
                "Victoria" => RGB::new(0, 160, 223),
                "Waterloo & City" => RGB::new(107, 205, 178),
                _ => fix_background_colour_rgb(background),
            },

            _ => fix_background_colour_rgb(background),
        },
        "f-ez-renfeoperadora" => match route.short_name.as_deref() {
            Some("AVE") => RGB::new(214, 5, 95),
            _ => RGB::new(184, 183, 189),
        },
        "f-9q5b-longbeachtransit" => {
            match route.id.as_str() {
                "1" => RGB::new(247, 161, 129),
                "2" => RGB::new(228, 228, 23),
                "8" => RGB::new(0, 167, 78),
                "22" => RGB::new(145, 7, 3),
                "22" => RGB::new(145, 7, 3),
                "23" => RGB::new(145, 7, 3),
                //passport
                "37" => RGB::new(214, 28, 41),
                "41" => RGB::new(219, 46, 24),
                "45" => RGB::new(0, 134, 172),
                "46" => RGB::new(225, 50, 29),
                "51" => RGB::new(227, 27, 140),
                "61" => RGB::new(12, 31, 107),
                "71" => RGB::new(4, 82, 161),
                "91" => RGB::new(193, 153, 6),
                "92" => RGB::new(193, 153, 6),
                "93" => RGB::new(193, 153, 6),
                "94" => RGB::new(193, 153, 6),
                "101" => RGB::new(0, 142, 136),
                "102" => RGB::new(0, 142, 136),
                "103" => RGB::new(0, 142, 136),
                "104" => RGB::new(0, 142, 136),
                "111" => RGB::new(39, 127, 195),
                "112" => RGB::new(23, 176, 80),
                "121" => RGB::new(54, 163, 198),
                "131" => RGB::new(158, 120, 89),
                "151" => RGB::new(249, 165, 27),
                "171" => RGB::new(116, 41, 142),
                "172" => RGB::new(116, 41, 142),
                "173" => RGB::new(116, 41, 142),
                "174" => RGB::new(116, 41, 142),
                "175" => RGB::new(116, 41, 142),
                "182" => RGB::new(155, 43, 103),
                "191" => RGB::new(2, 127, 61),
                "192" => RGB::new(2, 127, 61),
                "405" => RGB::new(0, 181, 236),
                _ => fix_background_colour_rgb(background),
            }
        }
        "f-9q5-metro~losangeles" => match background == WHITE_RGB {
            true => RGB::new(225, 103, 16),
            false => {
                let metroid = &route.id.split('-').collect::<Vec<&str>>()[0];

                match metroid.len() == 3 && metroid.starts_with('7') {
                    true => RGB::new(209, 18, 66),
                    false => fix_background_colour_rgb(background),
                }
            }
        },
        "f-9-amtrak~amtrakcalifornia~amtrakcharteredvehicle" => RGB::new(23, 114, 172),
        "f-9mu-mts" => match route.id.as_str() {
            "280" => RGB::new(7, 103, 56),
            "290" => RGB::new(235, 194, 22),
            "237" => RGB::new(96, 41, 133),
            "201" => RGB::new(232, 93, 152),
            "202" => RGB::new(232, 93, 152),
            "204" => RGB::new(232, 93, 152),
            "235" => RGB::new(242, 0, 16),
            "215" => RGB::new(59, 192, 225),
            "225" => RGB::new(41, 52, 144),
            "227" => RGB::new(123, 194, 77),
            "510" => RGB::new(0, 112, 191),
            "520" => RGB::new(255, 143, 0),
            "530" => RGB::new(0, 171, 70),
            "535" => RGB::new(184, 115, 51),
            "888" | "891" | "892" | "894" => RGB::new(98, 54, 27),
            _ => RGB::new(220, 38, 38),
        },
        "f-c23-metrokingcounty" => match route.short_name.as_deref() {
            // RapidRide Red
            Some("A Line" | "B Line" | "C Line" | "D Line" | "E Line" | "F Line" | "H Line") => {
                RGB::new(180, 10, 54)
            }
            // Override default region-wide blue to separate from ST Express
            _ => RGB::new(41, 133, 107),
        },
        _ => fix_background_colour_rgb(background),
    }
}

pub fn fix_foreground_colour_rgb(background: RGB<u8>, foreground: RGB<u8>) -> RGB<u8> {
    if background == foreground {
        RGB::new(0, 0, 0)
    } else {
        foreground
    }
}

pub fn fix_foreground_colour_rgb_feed(
    feed_id: &str,
    background: RGB<u8>,
    foreground: RGB<u8>,
) -> RGB<u8> {
    match feed_id {
        "f-9q5b-longbeachtransit" => WHITE_RGB,
        _ => fix_foreground_colour_rgb(background, foreground),
    }
}
