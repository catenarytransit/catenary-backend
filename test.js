const url = new URL("https://cypress.catenarymaps.org/v2/reverse");
url.searchParams.append("point.lat", 47.37);
url.searchParams.append("point.lon", 8.54);
url.searchParams.append("size", 10.0);
console.log(url.toString());
