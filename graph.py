import csv
from graphviz import Graph

# Function to read the CSV file and create a graph
def create_interlinking_graph(csv_file):
    graph = Graph(comment='Interlinking Pages', format='svg', engine='twopi')
    node_colors = {}  # Dictionary to store node colors
    
    with open(csv_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            from_page = row['From Page']
            to_page = row['To Page']
            color = row['Color']
            if from_page not in node_colors:
                graph.node(from_page, URL=f'https://en.wikipedia.org{from_page}', color=color)
                node_colors[from_page] = color
            if to_page not in node_colors:
                graph.node(to_page, URL=f'https://en.wikipedia.org{to_page}', color=color)
                node_colors[to_page] = color
            graph.edge(from_page, to_page, color=color)
    
    return graph

# Function to save the graph as an interactive SVG file
def save_interlinking_graph(graph, output_file):
    graph.attr(size='!', ratio='fill', overlap='scale', splines='line', nodesep='0.1', ranksep='1')
    graph.render(output_file, view=True)

# Example usage
input_csv_file = 'wikipedia_data.csv'
output_svg_file = 'interlinking_graph'
graph = create_interlinking_graph(input_csv_file)
save_interlinking_graph(graph, output_svg_file)
