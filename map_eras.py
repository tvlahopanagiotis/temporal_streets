import wikipedia
import re
import osmnx as ox
import matplotlib.pyplot as plt
import concurrent.futures
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import random
import time
import pandas as pd
import folium

def year_to_century(year: int) -> str:
    """Convert a year to its corresponding century representation."""
    century = (year - 1) // 100 + 1
    suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(century if century < 4 else 4, 'th')
    return f"{century}{suffix} century"

def century_to_era(century: str) -> str:
    """Map a given century to a broader historical era."""
    century_num = int(re.search(r"(\d+)", century).group(1))
    era_mapping = [
        (range(1, 6), 'ancient'),
        (range(6, 16), 'medieval'),
        (range(16, 19), 'early modern'),
        (range(19, 21), 'modern')
    ]
    for century_range, era in era_mapping:
        if century_num in century_range:
            return era
    return 'contemporary' if century.lower().startswith('20th') else 'unknown'

def extract_era_and_context_from_wikipedia(street_name: str, city_name: str, recursion_count=0) -> tuple:
    """Extract historical context (era and reason) for a given street name and city from Wikipedia."""
    print(f"Processing: {street_name} in {city_name}...")
    era_patterns = [
        re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)? century\b', re.IGNORECASE),
        re.compile(r'\b(\d{4})\b')
    ]
    context_patterns = [
        re.compile(r'named (after|for) ([^.]+)\.', re.IGNORECASE),
        re.compile(r'commemorate[s]? (?:the|an?)? ?([^.]+)\.', re.IGNORECASE),
        re.compile(r'in honor of ([^.]+)\.', re.IGNORECASE),
        re.compile(r'celebrate[s]? (?:the|an?)? ?([^.]+)\.', re.IGNORECASE),
        re.compile(r'recognize[s]? ([^.]+)\.', re.IGNORECASE)
    ]
    try:
        query = f"{street_name}, {city_name}"
        page = wikipedia.page(query)
        content = page.content
        print(f"Found Wikipedia page for {street_name}. Extracting era and context...")
        era = None
        context = None
        for pattern in era_patterns:
            match = pattern.search(content)
            if match:
                era = match.group(1)
                if era.isdigit() and len(era) == 4:
                    era = year_to_century(int(era))
                break
        for pattern in context_patterns:
            match = pattern.search(content)
            if match:
                context = match.group(2) if len(match.groups()) == 2 else match.group(1)
                break
        return era, context
    except wikipedia.exceptions.DisambiguationError as e:
        if recursion_count < 3:
            print(f"Disambiguation error for {street_name}. Picking the first option...")
            return extract_era_and_context_from_wikipedia(e.options[0], city_name, recursion_count + 1)
        else:
            print(f"Multiple disambiguation errors for {street_name}. Skipping...")
            return None, None
    except Exception as e:
        print(f"Error processing {street_name}: {e}")
        return None, None

def extract_era_and_context_from_wikipedia_retry(street_name: str, city_name: str, retry_count: int = 0, max_retries: int = 3) -> tuple:
    """Extract historical context (era and reason) for a given street name and city from Wikipedia with retry mechanism."""
    try:
        # Introduce a random delay
        time.sleep(random.uniform(0.1, 0.5))
        
        return extract_era_and_context_from_wikipedia(street_name, city_name)
    except wikipedia.exceptions.DisambiguationError as e:
        if retry_count < max_retries:
            print(f"Disambiguation error for {street_name}. Retrying {retry_count + 1}/{max_retries}...")
            return extract_era_and_context_from_wikipedia_retry(e.options[0], city_name, retry_count + 1)
        else:
            print(f"Multiple disambiguation errors for {street_name}. Skipping...")
            return None, None
    except Exception as e:
        if retry_count < max_retries:
            print(f"Error processing {street_name}. Retrying {retry_count + 1}/{max_retries}...")
            return extract_era_and_context_from_wikipedia_retry(street_name, city_name, retry_count + 1)
        else:
            print(f"Error processing {street_name} after {max_retries} retries: {e}")
            return None, None

# def extract_street_eras_and_context_limited(city_name: str, country_name: str, limit: int = 100) -> dict:
def process_street(street: str, city_name: str) -> tuple:
    """Process a single street to determine its era and context."""
    era, context = extract_era_and_context_from_wikipedia_retry(street, city_name)
    if era:
        century_era = century_to_era(era)
        return (street, {'era': century_era, 'context': context})
    return None

def extract_street_eras_and_context_parallel_progressbar(city_name: str, country_name: str, max_workers: int = 10, limit: int = 100) -> dict:
# def extract_street_eras_and_context_parallel_progressbar(city_name: str, country_name: str, max_workers: int = 10) -> dict:
    """Retrieve street names for a specified city and determine the era and context for each street using parallel processing with a progress bar."""
    graph = ox.graph_from_place(f"{city_name}, {country_name}", network_type="drive")
    street_names = []
    for _, _, _, data in graph.edges(keys=True, data=True):
        if 'name' in data:
            if isinstance(data['name'], str):
                street_names.extend(data['name'].split('/'))
            else:
                street_names.extend(data['name'])
    
    # Limit the number of streets
    street_names = street_names[:limit]
    street_eras = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Initialize tqdm progress bar
        progress_bar = tqdm(total=len(street_names), desc="Processing streets")
        
        # Use a generator expression to process streets in parallel
        futures = {executor.submit(process_street, street, city_name): street for street in street_names}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                street, data = result
                street_eras[street] = data
            progress_bar.update(1)  # Manually update the progress bar
        progress_bar.close()

    return street_eras

def save_street_eras_to_csv(street_eras: dict, filename="street_eras.csv"):
    """Save the street and era associations to a .csv file."""
    
    # Convert the dictionary to a pandas DataFrame
    df = pd.DataFrame.from_dict(street_eras, orient='index')
    
    # Save the DataFrame to a .csv file
    df.to_csv(filename)


def visualize_and_save_street_eras(street_eras: dict, city_name: str, country_name: str, filename: str = "street_eras.png", dpi: int = 300):
    """Visualize the eras of streets on a map using color coding and save the result as a PNG."""
    # Define color map for different eras
    color_map = {
        'ancient': 'pink',
        'medieval': 'orange',
        'early modern': 'green',
        'modern': 'blue',
        'contemporary': 'purple',
        '20th century': 'cyan',
        'unknown': 'red'
    }
    
    # Configure OSMnx to download and plot street data for the specified city
    graph = ox.graph_from_place(f"{city_name}, {country_name}", network_type="drive")
    
    # Initialize a list to store edge colors based on street eras
    ec = []
    for _, _, _, data in graph.edges(keys=True, data=True):
        color_assigned = False
        if 'name' in data:
            street_names = data['name'].split('/') if isinstance(data['name'], str) else data['name']
            for street in street_names:
                if street in street_eras:
                    era = street_eras[street]['era']
                    ec.append(color_map.get(era, 'red'))
                    color_assigned = True
                    break
        if not color_assigned:
            ec.append('red')
    
    # Plot the graph with edge colors based on eras
    fig, ax = ox.plot_graph(
        graph, 
        edge_color=ec, 
        edge_linewidth=2, 
        node_size=0, 
        bgcolor='white', 
        show=False, 
        close=False
    )
    
    # Create a legend based on the extracted eras
    legend_elements = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10, label=era) 
                       for era, color in color_map.items()]
    
    plt.legend(handles=legend_elements, title="Eras", loc="upper left")
    plt.savefig(filename, dpi=dpi, bbox_inches='tight')
    plt.show()

def save_to_geospatial_files(graph, street_eras, filename_prefix="street_eras"):
    """
    Convert the graph to geospatial formats and save to disk.
    Fixes issues with unsupported data types and avoids geometry interference.
    
    Parameters:
    - graph: the OSMnx graph
    - street_eras: the dictionary with street era data
    - filename_prefix: the prefix for the saved files
    """
    nodes, edges = ox.graph_to_gdfs(graph)

    # Add an 'era' column to the edges GeoDataFrame
    edges['era'] = edges['name'].map(lambda x: street_eras.get(x, {}).get('era', 'unknown') if isinstance(x, str) else 'unknown')
    
    # Convert columns containing lists to strings, excluding the 'geometry' column
    non_geometry_columns = edges.columns.difference(['geometry'])
    for column in non_geometry_columns:
        if edges[column].apply(type).eq(list).any():
            edges[column] = edges[column].astype(str)
    
    # Save edges GeoDataFrame as GeoPackage (can also be saved as .shp or other formats)
    edges.to_file(f"{filename_prefix}.gpkg", driver="GPKG")
    
    return edges

def create_leaflet_map_with_history(edges_gdf, street_data, filename="leaflet_map.html"):
    """
    Create an enhanced Leaflet map using folium, with history pop-ups, and save to an HTML file.
    
    Parameters:
    - edges_gdf: the edges GeoDataFrame with street data
    - street_data: Dictionary containing street era and context data.
    - filename: the name of the HTML file to save
    """
    # Initialize the map centered around the mean coordinates of the data
    m = folium.Map(location=[edges_gdf.geometry.unary_union.centroid.y, edges_gdf.geometry.unary_union.centroid.x], 
                   zoom_start=14, 
                   tiles="CartoDB positron")

    # Add title and description using HTML
    title_html = '''
                 <h3 align="center" style="font-size:20px"><b>A walk through history: Temporal streets in Oxford, UK</b></h3>
                 <h4 align="center" style="font-size:16px">Deriving the historical context of street names using OSM & Wikipedia</h4>
                 '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    # Define color map for different eras
    color_map = {
        'ancient': 'gray',
        'medieval': 'orange',
        'early modern': 'green',
        'modern': 'blue',
        'contemporary': 'purple',
        'unknown': 'red'
    }

    # Add streets to the map with enhanced styling and pop-ups
    for _, row in edges_gdf.iterrows():
        line_color = color_map.get(row['era'], 'red')
        street_history = street_data.get(row['name'], {}).get('context', 'No historical context available.')
        popup_content = f"Street: {row['name']}<br>Era: {row['era']}<br>History: {street_history}"
        popup = folium.Popup(popup_content, max_width=500)
        folium.PolyLine([(x[1], x[0]) for x in list(row['geometry'].coords)], 
                        color=line_color, 
                        weight=2.5,
                        opacity=0.8,
                        popup=popup).add_to(m)

    # Add a legend to the map
    legend_html = '''
        <div style="position: fixed; bottom: 50px; left: 10px; width: 150px; height: 200px; 
                    background-color: white; border:2px solid grey; z-index:9999; font-size:14px;">
        &nbsp;<b>Street Eras:</b><br>
        &nbsp;Ancient: &nbsp;<i class="fa fa-minus" style="color:red"></i><br>
        &nbsp;Medieval: &nbsp;<i class="fa fa-minus" style="color:orange"></i><br>
        &nbsp;Early Modern: &nbsp;<i class="fa fa-minus" style="color:green"></i><br>
        &nbsp;Modern: &nbsp;<i class="fa fa-minus" style="color:blue"></i><br>
        &nbsp;Contemporary: &nbsp;<i class="fa fa-minus" style="color:purple"></i><br>
        &nbsp;Unknown: &nbsp;<i class="fa fa-minus" style="color:red"></i>
        </div>
        '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Save map to an HTML file
    m.save(filename)


city = "Oxford"
country = "United Kingdom"
street_data = extract_street_eras_and_context_parallel_progressbar(city, country)
visualize_and_save_street_eras(street_data, city, country)
save_street_eras_to_csv(street_data)
# Usage:
graph = ox.graph_from_place(f"{city}, {country}", network_type="drive")
edges_gdf = save_to_geospatial_files(graph, street_data)
create_leaflet_map_with_history(edges_gdf,street_data,"leaflet_map_with_history.html")
