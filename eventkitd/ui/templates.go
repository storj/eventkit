package ui

import (
	"html/template"
)

var tmplMain = template.Must(template.New("").Parse(`<!doctype html>
<html>
<head><title>Eventkitd</title>
<style>
dt {
  float: left;
  clear: left;
  padding-right: 10px;
}
</style>
</head>
<body>
<dl>
  <dt>Target:</dt>
  <dd><select id="target">
      {{ range .Targets }}
        <option>{{.}}</option>
      {{ end }}
      </select></dd>

  <dt>Graph:</dt>
  <dd><select id="graph_type">
      {{ range .Graphs }}
        <option value="{{ .ShortName }}">{{ .Name }}</option>
      {{ end }}
      </select></dd>

  <dt>From:</dt>
  <dd><input type="text" id="from" value="1 day ago"/></dd>

  <dt>Until:</dt>
  <dd><input type="text" id="until"/></dd>

  <dt class="column-field">Column:</dt>
  <dd class="column-field"><input type="text" id="column"/></dd>

</dl>
<p><input type="submit" value="Graph" id="graph-button"/></p>
<p><i>todo: ui for adding processors, other fields, but gsd</i></p>

<script src="//ajax.googleapis.com/ajax/libs/jquery/1.10.1/jquery.min.js">
  </script>
<script>
$("#graph_type").change(function() {
  $(".column-field").hide()
  {{ range $i, $graph := .Graphs }}
	{{ range .Fields }}
      {{ if (eq .Field "column") }}
        if ($(this).val() == "{{ $graph.ShortName }}") {
          $(".column-field").show()
        }
      {{ end }}
    {{ end }}
  {{ end }}
});
$("#graph-button").click(function() {
  var params = {
      "target": $("#target").val() || "",
      "type": $("#graph_type").val() || "",
      "from": $("#from").val() || "",
      "until": $("#until").val() || "",
      "column": $("#column").val() || ""}
  for (var name in params) {
    if (params[name].length == 0) {
      delete params[name]
    }
  }
  location.href = "../render/?" + $.param(params);
});
$(document).ready(function() {
  $("#graph_type").change()
});
</script>
</body>
</html>
`))
