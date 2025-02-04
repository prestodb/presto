const path = window.location.pathname;

// when access the page /dev/query_viewer_spa.html page
if ( path.indexOf("/dev/") >= 0 ){
    __webpack_public_path__ = "../";
}
else {
    __webpack_public_path__ = "./";
}