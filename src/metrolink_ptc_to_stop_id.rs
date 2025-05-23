//stop_id metrolink gtfs -> ptc
pub const METROLINK_STOP_LIST: [(&str, &str); 68] = [
    ("191", "REDLANDS-ESRI"),
    ("190", "REDLANDS-DOWNTOWN-ARROW"),
    ("189", "REDLANDS-ESRI"),
    ("188", "SANBERNARDINO-TIPPECANOE"),
    ("187", "VENTURA-DOWNTOWN"),
    ("186", "BURBANK-AIRPORT-NORTH"),
    //san bernardino downtown
    ("185", "SANBERNARDINOTRAN"),
    ("184", "PERRIS-SOUTH"),
    ("183", "PERRIS-DOWNTOWN"),
    ("182", "MORENO-VALLEY-MARCH-FIELD"),
    ("181", "RIVERSIDE-HUNTERPARK"),
    ("175", "NEWHALL"),
    ("174", "FULLERTON"),
    ("173", "CORONA-WEST"),
    ("171", "ANAHEIM-CANYON"),
    ("170", "BURBANK-AIRPORT-SOUTH"),
    ("169", "VENTURA-EAST"),
    ("168", "OXNARD"),
    ("167", "NORTHRIDGE"),
    ("166", "CAMARILLO"),
    ("165", "VINCENT GRADE/ACTON"),
    ("164", "VIA PRINCESSA"),
    ("163", "PALMDALE"),
    ("162", "LANCASTER"),
    ("161", "VISTA-CANYON"),
    ("156", "TUSTIN"),
    ("154", "SANTA ANA"),
    ("153", "SAN JUAN CAPISTRANO"),
    ("152", "SAN CLEMENTE"),
    ("148", "RIVERSIDE-LA SIERRA"),
    ("147", "MAIN-CORONA-NORTH"),
    ("145", "ORANGE"),
    ("144", "OCEANSIDE"),
    ("143", "NORWALK-SANTAFESPRINGS"),
    ("141", "LAGUNANIGUEL-MISSIONVIEJO"),
    ("140", "IRVINE"),
    ("135", "COMMERCE"),
    ("133", "SAN CLEMENTE PIER"),
    ("130", "BUENAPARK"),
    ("129", "SUN VALLEY"),
    //anaheim
    ("128", "ARTIC"),
    ("127", "INDUSTRY"),
    ("126", "MONTEBELLO"),
    ("125", "UPLAND"),
    //depot
    ("124", "SANBERNARDINO"),
    ("123", "RIVERSIDE-DOWNTOWN"),
    ("122", "RIALTO"),
    ("121", "RANCHO CUCAMONGA"),
    ("120", "POMONA-DOWNTOWN"),
    ("119", "PEDLEY"),
    ("118", "MONTCLAIR"),
    ("117", "ONTARIO-EAST"),
    ("116", "FONTANA"),
    ("115", "CALSTATE"),
    ("114", "CLAREMONT"),
    ("113", "VAN NUYS"),
    ("112", "SIMIVALLEY"),
    ("111", "SANTA CLARITA"),
    ("110", "SYLMAR/SAN FERNANDO"),
    ("109", "POMONA-NORTH"),
    ("108", "MOORPARK"),
    ("107", "LAUS"),
    ("106", "GLENDALE"),
    ("105", "ELMONTE"),
    ("104", "COVINA"),
    ("103", "CHATSWORTH"),
    ("102", "DOWNTOWN BURBANK"),
    ("101", "BALDWINPARK"),
];

//amtrak stop id gtfs -> ptc
pub const AMTRAK_STOP_TO_SCAX_PTC_LIST: [(&str, &str); 21] = [
    //oceanside
    ("OSD", "OCEANSIDE"),
    //san juan capistrano
    ("SNC", "SAN JUAN CAPISTRANO"),
    //irvine
    ("IRV", "IRVINE"),
    //santa ana (SNA)
    ("SNA", "SANTA ANA"),
    //anaheim
    ("ANA", "ARTIC"),
    //fullerton
    ("FUL", "FULLERTON"),
    //los angeles
    ("LAX", "LAUS"),
    //glendale
    ("GDL", "GLENDALE"),
    //burbank downtown
    ("BBK", "DOWNTOWN BURBANK"),
    //burbank south vc line & amtrak
    ("BUR", "BURBANK-AIRPORT-SOUTH"),
    //van nuys
    ("VNC", "VAN NUYS"),
    //northridge
    ("NRG", "NORTHRIDGE"),
    //chatsworth
    ("CWT", "CHATSWORTH"),
    //simi valley
    ("SIM", "SIMIVALLEY"),
    //moorpark
    ("MPK", "MOORPARK"),
    //Camarillo
    ("CML", "CAMARILLO"),
    //oxnard
    ("OXN", "OXNARD"),
    //ventura downtown
    ("VEC", "VENTURA-DOWNTOWN"),
    //san bernardino depot
    ("SNB", "SANBERNARDINO"),
    //riverside amtrak downtown
    ("RIV", "RIVERSIDE-DOWNTOWN"),
    //downtown pomona amtrak station
    ("POS", "POMONA-DOWNTOWN"), //ontario station amtrak is not shared with metrolink, do not include
];
