<%
new com.psddev.dari.util.DebugFilter.PageWriter(application, request, response) {{
    startPage("Welcome!");
        start("div", "class", "hero-unit", "style", "background: transparent; margin: 0 auto; padding-left: 0; padding-right: 0; width: 50em;");
            start("h2", "style", "margin-bottom: 20px;").html("Congratulations on installing Dari!").end();
            start("p").html("Dari is a powerful data modeling framework that makes it easy to work with complex data structures and persist them to one or more database backends. It's been carefully crafted over years of experience with real-world challenges.").end();
            start("p", "style", "margin-top: 30px;");
                start("a",
                        "class", "btn btn-large",
                        "href", "http://www.dariframework.org/documentation.html");
                    html("Let's get started \u2192");
                end();
            end();
        end();
    endPage();
}};
%>
