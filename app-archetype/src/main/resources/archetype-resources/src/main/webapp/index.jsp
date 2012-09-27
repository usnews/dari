<%
new com.psddev.dari.util.DebugFilter.PageWriter(application, request, response) {{
    startPage("Welcome!");
        start("div", "class", "hero-unit", "style", "background: transparent; margin: 0 auto; padding-left: 0; padding-right: 0; width: 50em;");
            start("h2", "style", "margin-bottom: 20px;").html("Congratulations on installing Dari!").end();
            start("p").html("Dari is an intuitive Java development framework that takes care of a wide range of peripheral tasks to let developers focus on their application. Crafted over years of large-scale problem solving, Dari brings professional best practices into every developer's workflow.").end();
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
