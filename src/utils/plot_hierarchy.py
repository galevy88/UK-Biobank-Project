import yaml
import plotly.graph_objects as go
import os
import json


def read_yaml_file(yaml_file_path):
    """Read the YAML file."""
    with open(yaml_file_path, 'r') as file:
        return yaml.safe_load(file)


def flatten_hierarchy(data):
    """Flatten YAML hierarchy into IDs, labels, and parents with level-specific codes."""
    ids, labels, parents = [], [], []
    ids.append("CategoryCount")
    labels.append("Disease Hierarchy")
    parents.append("")
    for chapter, chapter_data in data.get('CategoryCount', {}).items():
        chapter_id = chapter
        chapter_label = chapter_data.get('title', chapter)
        ids.append(chapter_id)
        labels.append(chapter_label)
        parents.append("CategoryCount")
        for subcategory_code, subcategory_data in chapter_data.get('subcategories', {}).items():
            subcategory_id = subcategory_code
            subcategory_label = subcategory_data.get('title', subcategory_code)
            ids.append(subcategory_id)
            labels.append(subcategory_label)
            parents.append(chapter_id)
            for code, code_data in subcategory_data.get('subcategories', {}).items():
                code_id = code
                code_label = code_data.get('title', code)
                ids.append(code_id)
                labels.append(code_label)
                parents.append(subcategory_code)
                for sub_code, sub_title in code_data.get('subcategories', {}).items():
                    sub_code_id = sub_code
                    ids.append(sub_code_id)
                    labels.append(sub_title)
                    parents.append(code)
    return ids, labels, parents


def plot_hierarchy(yaml_file_path, output_html_path):
    """Create an interactive treemap with selection mechanism."""
    yaml_data = read_yaml_file(yaml_file_path)
    ids, labels, parents = flatten_hierarchy(yaml_data)

    # Initialize selection state and colors
    selection_states = {id_: 0 for id_ in ids}  # 0: undecided, 1: want (green), 2: don't want (red)
    colors = ["#D3D3D3"] * len(ids)  # Default color

    fig = go.Figure(go.Treemap(
        ids=ids,
        labels=ids,  # Use IDs (codes) as visible labels
        parents=parents,
        textinfo="label",
        hoverinfo="text",
        hovertext=labels,  # Use names as hover text
        root_color="#F0F0F0",
        marker=dict(colors=colors, line=dict(width=1, color='white')),
        tiling=dict(packing='squarify')
    ))

    fig.update_layout(
        title={'text': "Interactive Disease Hierarchy",
               'font': {'size': 28, 'family': 'Arial, sans-serif', 'color': '#2c3e50'}},
        margin=dict(t=80, l=40, r=40, b=40),
        width=1400,
        height=1000,
        font=dict(size=14, family='Arial, sans-serif'),
        plot_bgcolor='#ECF0F1',
        paper_bgcolor='#ECF0F1',
        showlegend=False
    )
    fig.update_traces(textfont=dict(size=14), marker_line_width=1, insidetextfont=dict(size=10))

    # Add JavaScript for click handling
    js_code = f"""
    <script>
    document.addEventListener('DOMContentLoaded', function() {{
        var plotDiv = document.getElementsByClassName('plotly-graph-div')[0];
        var selectionStates = {json.dumps(selection_states)};
        var originalColors = {json.dumps(colors)};
        var plotIds = {json.dumps(ids)};
        var plotParents = {json.dumps(parents)};
        var currentPoint = null;

        // Build children map
        var childrenMap = {{}};
        for (var i = 0; i < plotIds.length; i++) {{
            var p = plotParents[i];
            if (p) {{
                if (!childrenMap[p]) childrenMap[p] = [];
                childrenMap[p].push(plotIds[i]);
            }}
        }}

        // Function to set state recursively
        function setStateRecursive(id, state) {{
            selectionStates[id] = state;
            if (childrenMap[id]) {{
                childrenMap[id].forEach(function(child) {{
                    setStateRecursive(child, state);
                }});
            }}
        }}

        // Function to get selected leaves
        function getSelectedLeaves() {{
            var leaves = [];
            for (var key in selectionStates) {{
                if (selectionStates[key] === 1 && (!childrenMap[key] || childrenMap[key].length === 0)) {{
                    leaves.push(key);
                }}
            }}
            leaves.sort();
            return leaves;
        }}

        // Function to get rejected leaves
        function getRejectedLeaves() {{
            var leaves = [];
            for (var key in selectionStates) {{
                if (selectionStates[key] === 2 && (!childrenMap[key] || childrenMap[key].length === 0)) {{
                    leaves.push(key);
                }}
            }}
            leaves.sort();
            return leaves;
        }}

        plotDiv.on('plotly_hover', function(data) {{
            currentPoint = data.points[0];
        }});

        plotDiv.on('plotly_unhover', function(data) {{
            currentPoint = null;
        }});

        plotDiv.addEventListener('contextmenu', function(event) {{
            if (currentPoint) {{
                event.preventDefault();
                var id = currentPoint.id;
                var currentState = selectionStates[id];
                var newState = (currentState + 1) % 3;
                setStateRecursive(id, newState);
                var colors = originalColors.slice();
                for (var key in selectionStates) {{
                    if (selectionStates[key] === 1) {{
                        colors[plotIds.indexOf(key)] = 'rgba(144, 238, 144, 0.8)';  // Soft green for "want"
                    }} else if (selectionStates[key] === 2) {{
                        colors[plotIds.indexOf(key)] = 'rgba(240, 128, 128, 0.8)';  // Soft red for "don't want"
                    }}
                }}
                Plotly.restyle(plotDiv, {{ 'marker.colors': [colors] }}, [0]);
            }}
        }});

        // Create save button
        var saveButton = document.createElement('button');
        saveButton.innerText = 'Save Selected';
        saveButton.style.position = 'absolute';
        saveButton.style.top = '10px';
        saveButton.style.right = '10px';
        document.body.appendChild(saveButton);

        // Save button click handler
        saveButton.addEventListener('click', function() {{
            var wanted = getSelectedLeaves();
            var unwanted = getRejectedLeaves();

            var yamlContentWanted = 'codes:\\n' + wanted.map(function(id) {{ return '  - ' + id; }}).join('\\n');
            var blobWanted = new Blob([yamlContentWanted], {{ type: 'text/yaml' }});
            var urlWanted = URL.createObjectURL(blobWanted);
            var aWanted = document.createElement('a');
            aWanted.href = urlWanted;
            aWanted.download = 'wanted_diseases.yaml';
            document.body.appendChild(aWanted);
            aWanted.click();
            document.body.removeChild(aWanted);
            URL.revokeObjectURL(urlWanted);

            var yamlContentUnwanted = 'codes:\\n' + unwanted.map(function(id) {{ return '  - ' + id; }}).join('\\n');
            var blobUnwanted = new Blob([yamlContentUnwanted], {{ type: 'text/yaml' }});
            var urlUnwanted = URL.createObjectURL(blobUnwanted);
            var aUnwanted = document.createElement('a');
            aUnwanted.href = urlUnwanted;
            aUnwanted.download = 'unwanted_diseases.yaml';
            document.body.appendChild(aUnwanted);
            aUnwanted.click();
            document.body.removeChild(aUnwanted);
            URL.revokeObjectURL(urlUnwanted);
        }});
    }});
    </script>
    """

    # Write HTML with embedded JavaScript
    output_dir = os.path.dirname(output_html_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Generate HTML with JavaScript
    html_content = fig.to_html(full_html=True, include_plotlyjs='cdn')
    html_content = html_content.replace('</body>', js_code + '</body>')
    with open(output_html_path, 'w') as f:
        f.write(html_content)


def main():
    yaml_file_path = 'data/disease_tree_plot/output/tree_yaml.yaml'
    output_html_path = 'data/disease_tree_plot/output/disease_hierarchy.html'
    plot_hierarchy(yaml_file_path, output_html_path)


if __name__ == "__main__":
    main()