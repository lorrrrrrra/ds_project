// global variables
let reviews_grouped = [];
let reviews_grouped_month = [] ;
let reviews_grouped_year = [];
let reviews_grouped_price = [];
let activeMarker = null;

let filters = {
  general: 0,
  food: 0,
  service: 0,
  atmosphere: 0,
  price: [],
  type: []
}

let types = ["bakery", "bar", "breakfast_restaurant", "brunch_restaurant", "buffet_restaurant", "cafe", "fast_food_restaurant", 
  "fine_dining_restaurant", "meal_delivery", "meal_takeaway","vegan_restaurant", "vegetarian_restaurant", 
  "african_restaurant", "asian_restaurant", "chinese_restaurant", "indian_restaurant", "italian_restaurant", "seafood_restaurant", 
  "sushi_restaurant"]







// all things map
// loading of the map
const map = L.map('map').setView([48.52, 9.05], 13);
    
const tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);

let markersLayer = L.layerGroup().addTo(map); // Eine Markergruppe für alle Marker


// new Icons
var defaultIcon = L.icon({
  iconUrl: 'static/marker_green.png', // Standardmarker
  iconSize: [37.5, 58],
  iconAnchor: [22, 94],
  shadowAnchor: [4, 62],
  popupAnchor: [-3, -76],
});

var clickedIcon = L.icon({
  iconUrl: 'static/marker_orange.png', // Marker bei Klick
  iconSize: [37.5, 58],
  iconAnchor: [22, 94],
  shadowAnchor: [4, 62],
  popupAnchor: [-3, -76],
});

var deactivatedIcon = L.icon({
  iconUrl: 'static/marker_grey.png', // Marker wenn nicht bei filter
  iconSize: [37.5, 58],
  iconAnchor: [22, 94],
  shadowAnchor: [4, 62],
  popupAnchor: [-3, -76],
});

map.on('moveend', update_map); // Wenn die Karte verschoben wird
map.on('zoomend', update_map); // Wenn der Zoom geändert wird

// first initial update of map
update_map();

function update_map() {
  // get bound_values
  let bounds = map.getBounds();

  let bound_values = {
    lowLat: bounds.getSouth(),
    highLat: bounds.getNorth(),
    lowLng: bounds.getWest(),
    highLng: bounds.getEast()
  };

  let boundsString = `${bound_values.lowLat},${bound_values.highLat},${bound_values.lowLng},${bound_values.highLng}`;

  let currentActiveMarker = null;
  if (activeMarker) {
    currentActiveMarker = activeMarker;
  }
  
  // Fetch-Aufruf to get restaurants
  fetch(`/api/restaurants/${boundsString}`, {
      method: 'POST',  // Setze die Methode auf 'POST'
      headers: {
          'Content-Type': 'application/json'  
      },
      body: JSON.stringify(filters)  
  })
    .then(response => response.json())
    .then(data => {
      // Marker-Set löschen, bevor neue hinzugefügt werden
      markersLayer.clearLayers();

      // creating new markers
      create_marker(data);
    })
    .catch(error => {
      console.error("Fehler beim Abrufen der Daten:", error);
    });

    // keeping active Marker
    if (currentActiveMarker) {
      activeMarker = currentActiveMarker;
      currentActiveMarker.setIcon(clickedIcon);
    }
}




function create_marker(data) {
  data.forEach(item => {
    id = item.restaurant_id;
    lat_value = item.lat_value;
    long_value = item.long_value;
    filtered = item.filtered;

    let marker; 

    if (!isNaN(lat_value) && !isNaN(long_value)) {
      if (filtered) {
        marker = L.marker([lat_value, long_value], { id: id, icon: defaultIcon, filtered: filtered})
        .addTo(markersLayer);
        marker.setZIndexOffset(1000);
      } else if (!filtered) {
        marker = L.marker([lat_value, long_value], { id: id, icon: deactivatedIcon, filtered: filtered})
        .addTo(markersLayer);
        marker.setZIndexOffset(500);
      }
      marker.on('click', (e) => {
        if (activeMarker && activeMarker.options.filtered) {
          activeMarker.setIcon(defaultIcon);
          marker.setZIndexOffset(1000);
        } else if (activeMarker && !activeMarker.options.filtered) {
          activeMarker.setIcon(deactivatedIcon);
          marker.setZIndexOffset(500);
        }
        activeMarker = marker;
        marker.setIcon(clickedIcon);
        marker.setZIndexOffset(1500);   // to show in front of the other markers
        open_sidebar();
        handleMarkerClick(e.target.options.id); // ID des Markers verwenden
      });
    } else {
      console.error('Ungültige Koordinaten:', restaurant);
    }
  });
}











//initiate filter buttons for types
display_food_type_buttons(types);










fetch('static/csv files/reviews_grouped.csv')
.then((response) => response.text())
.then((csvData) => {
  // CSV parsen
  Papa.parse(csvData, {
    header: true, // Erste Zeile als Header interpretieren
    complete: (results) => {
      reviews_grouped = results.data; // Speichern der geparsten Restaurants
    },
    error: (error) => console.error('Fehler beim Parsen der CSV:', error),
  });
})
.catch((error) => console.error('Fehler beim Laden der CSV:', error));


fetch('static/csv files/reviews_grouped_month.csv')
.then((response) => response.text())
.then((csvData) => {
  // CSV parsen
  Papa.parse(csvData, {
    header: true, // Erste Zeile als Header interpretieren
    complete: (results) => {
      reviews_grouped_month = results.data; // Speichern der geparsten Restaurants
    },
    error: (error) => console.error('Fehler beim Parsen der CSV:', error),
  });
})
.catch((error) => console.error('Fehler beim Laden der CSV:', error));


fetch('static/csv files/reviews_grouped_year.csv')
.then((response) => response.text())
.then((csvData) => {
  // CSV parsen
  Papa.parse(csvData, {
    header: true, // Erste Zeile als Header interpretieren
    complete: (results) => {
      reviews_grouped_year = results.data; // Speichern der geparsten Restaurants
    },
    error: (error) => console.error('Fehler beim Parsen der CSV:', error),
  });
})
.catch((error) => console.error('Fehler beim Laden der CSV:', error));


fetch('static/csv files/dining_price_range_group.csv')
.then((response) => response.text())
.then((csvData) => {
  // CSV parsen
  Papa.parse(csvData, {
    header: true, // Erste Zeile als Header interpretieren
    complete: (results) => {
      reviews_grouped_price = results.data; // Speichern der geparsten Restaurants
    },
    error: (error) => console.error('Fehler beim Parsen der CSV:', error),
  });
})
.catch((error) => console.error('Fehler beim Laden der CSV:', error));




// Function to close the sidebar
function close_sidebar() {
  const sidebar = document.getElementById("rightSidebar");
  const mapContainer = document.getElementById("map"); // ID des Kartencontainers
  if (sidebar) {
    sidebar.classList.add("hidden"); // Versteckt die Sidebar

    // deleting active marker
    activeMarker.setIcon(defaultIcon); // 
    activeMarker = null;

    // resizing map
    setTimeout(() => {
        if (map) {
          map.invalidateSize(); // Aktualisiert die Kartengröße
        }
    }, 300); // Wartezeit, um sicherzustellen, dass die Transition abgeschlossen ist
  }
}

// Function to open the sidebar
function open_sidebar() {
  const sidebar = document.querySelector(".stripe-right");
  const mapContainer = document.getElementById("map");
  if (sidebar) {
      sidebar.classList.remove("hidden"); // Zeigt die Sidebar wieder an
      setTimeout(() => {
          if (map) {
              map.invalidateSize(); // Aktualisiert die Kartengröße
          }
      }, 300); // Wartezeit für die Transition
  }
}







  // all things details side
  function get_star_rating(data) {  
    if (data) {
      // Allgemeine Sternebewertung
      const star_rating = parseFloat(data.rating_overall);
      const star_rating_food = parseFloat(data.rating_food);
      const star_rating_service = parseFloat(data.rating_service);
      const star_rating_atmosphere = parseFloat(data.rating_atmosphere);
      const user_count_overall_google = parseFloat(data.user_count_overall_google);
      const user_count_food = parseFloat(data.user_count_food);
      const user_count_service = parseFloat(data.user_count_service);
      const user_count_atmosphere = parseFloat(data.user_count_atmosphere);

  
      // Durchschnittliche Bewertungen aktualisieren
    const updateAverageRating = (rating, group, user_count) => {
      const avgRatingElement = document.getElementById(`avg-rating${group}`);
      const avgCountElement = document.getElementById(`count-rating${group}`);
      if (!isNaN(rating)) {
        avgRatingElement.textContent = ` (${rating.toFixed(1)})`;
        avgCountElement.textContent = ` based on ${user_count} reviews`;
      } else {
        avgRatingElement.textContent = `(No rating available)`;
        avgCountElement.textContent = ` `;
      }
    };

    updateAverageRating(star_rating, '', user_count_overall_google); // Allgemeine Bewertung
    // updateAverageRating(star_rating_food, '-food-detail'); // Food-Bewertung
    // updateAverageRating(star_rating_service, '-service-detail'); // Service-Bewertung
    // updateAverageRating(star_rating_atmosphere, '-atmosphere-detail'); // Atmosphere-Bewertung
    updateAverageRating(star_rating_food, '-food', user_count_food); // Food-Bewertung
    updateAverageRating(star_rating_service, '-service', user_count_service); // Service-Bewertung
    updateAverageRating(star_rating_atmosphere, '-atmosphere', user_count_atmosphere); // Atmosphere-Bewertung
  
      // Helper-Funktion zum Aktualisieren der Sterne
      const updateStars = (rating, group) => {
        const stars = [
          document.getElementById(`star-1${group}`),
          document.getElementById(`star-2${group}`),
          document.getElementById(`star-3${group}`),
          document.getElementById(`star-4${group}`),
          document.getElementById(`star-5${group}`),
        ];


  
        const fullStars = Math.floor(rating);
        const hasHalfStar = (rating % 1) >= 0.26 && (rating % 1) < 0.75;
        const hasFullStar = (rating % 1) >= 0.75;
  
        stars.forEach((star, index) => {
          if (index < fullStars) {
            star.src = "static/graphics/voller_stern.png"; // Voller Stern
          } else if (index === fullStars && hasHalfStar) {
            star.src = "static/graphics/halber_stern.png"; // Halber Stern
          } else if (index === fullStars && hasFullStar) {
            star.src = "static/graphics/voller_stern.png"; // Voller Stern
          } else {
            star.src = "static/graphics/leerer_stern.png"; // Leerer Stern
          }
        });
      };
  
      // Sterne für jede Gruppe aktualisieren
      updateStars(star_rating, ''); // Allgemeine Sterne
      // updateStars(star_rating_food, '-food-detail'); // Food
      // updateStars(star_rating_service, '-service-detail'); // Service
      // updateStars(star_rating_atmosphere, '-atmosphere-detail'); // Atmosphere
      updateStars(star_rating_food, '-food'); // Food
      updateStars(star_rating_service, '-service'); // Service
      updateStars(star_rating_atmosphere, '-atmosphere'); // Atmosphere
  
    } else {
      console.error(`Kein Restaurant gefunden.`);
    }
  }


function get_summaries(data) {
  // Restaurant mit passender ID suchen
  if (data) {
    const summary_placeholder = "Unfortunately, there were no written reviews to base our summary upon.";

    const summary_overall = data.summary_overall || summary_placeholder;
    const summary_food = data.summary_food || summary_placeholder;
    const summary_service = data.summary_service || summary_placeholder;
    const summary_atmosphere = data.summary_atmosphere || summary_placeholder;
    const summary_price = data.summary_price || summary_placeholder;

    let formatted_summary_food = summary_food
      .replace(/\n/g, '<div>')  // Ersetze \n mit <p> für Absätze
      .replace(/\n-/g, '<ul><li>')  // Ersetze \n- mit <ul><li> für Listenelemente
      .replace(/<\/li><ul>/g, '</li></ul>')  // Korrigiere geschachtelte Tags
      .replace(/\n/g, '</li><ul>') // Korrigiere das Tag vor dem ersten Listenelement
      .replace(/\*/g, '');

    const text_count_overall = data.summary_overall ? `generated with AI - based on ${parseFloat(data.user_count_overall) || 0} reviews` : "";
    const text_count_food = data.summary_food ? `generated with AI - based on ${parseFloat(data.user_count_food) || 0} reviews` : "";
    const text_count_service = data.summary_service ? `generated with AI - based on ${parseFloat(data.user_count_service) || 0} reviews` : "";
    const text_count_atmosphere = data.summary_atmosphere ? `generated with AI - based on ${parseFloat(data.user_count_atmosphere) || 0} reviews` : "";
    const text_count_price = data.summary_price ? `generated with AI - based on ${parseFloat(data.user_count_price) || 0} reviews` : "";

    const html_summary_overall = document.getElementById('summary');
    const html_summary_food = document.getElementById('summary-food');
    const html_summary_service = document.getElementById('summary-service');
    const html_summary_atmosphere = document.getElementById('summary-atmosphere');
    const html_summary_price = document.getElementById('summary-price');

    const label_amount_overall = document.getElementById('label-amount-overall');
    const label_amount_food = document.getElementById('label-amount-food');
    const label_amount_service = document.getElementById('label-amount-service');
    const label_amount_atmosphere = document.getElementById('label-amount-atmosphere');
    const label_amount_price = document.getElementById('label-amount-price');

    html_summary_overall.textContent = summary_overall;
    html_summary_food.innerHTML = formatted_summary_food;
    html_summary_service.textContent = summary_service;
    html_summary_atmosphere.textContent = summary_atmosphere;
    html_summary_price.textContent = summary_price;

    label_amount_overall.textContent = text_count_overall;
    label_amount_food.textContent = text_count_food;
    label_amount_service.textContent = text_count_service;
    label_amount_atmosphere.textContent = text_count_atmosphere;
    label_amount_price.textContent = text_count_price;
  }
}


function get_type_tags (type_tags) {
  type_tags = JSON.parse(type_tags.replace(/'/g, '"'));
  const container = document.getElementById("type_tags");   //container on the html in which the badges will go
  container.innerHTML = ""; //deleting everything in it
  let type_to_show = [];

  for (const element of type_tags) {
    //checking if element is in list of predefined tags we want to show 
    if (types.includes(element)) {
      type_to_show.push(element);
    }
  }

  type_to_show = type_to_show.map(element => {
    // "_restaurant" entfernen
    let no_restaurant = element.replace("_restaurant", "");
    // "_" durch Leerzeichen ersetzen
    return no_restaurant.replace(/_/g, " ");
  });

  type_to_show.forEach(item => {
    // Ein neues Badge-Element erstellen
    const badge = document.createElement("span");
    badge.className = "badge rounded-pill bg-type me-2 mt-2"; // Bootstrap-Klassen
    badge.textContent = item; // Textinhalt setzen
    
    // Badge dem Container hinzufügen
    container.appendChild(badge);
  });
  // console.log(type_to_show);
}


function get_details(data) {
  const sidebarName = document.getElementById('name');
  const sidebarAddress = document.getElementById('address');
  const sidebarWebsiteUrl = document.getElementById('website-url');

  sidebarName.textContent = data.name;
  sidebarAddress.textContent = data.address;

  if (data.website_uri !== null && data.website_uri !== undefined && data.website_uri != "NaN") {
    sidebarWebsiteUrl.textContent = "Click here to access website";
    sidebarWebsiteUrl.href = data.website_uri;
    // sidebarWebsiteUrl.target = "_blank";
  } else {
    sidebarWebsiteUrl.textContent = "";
    sidebarWebsiteUrl.href = "";
    // sidebarWebsiteUrl.target = "";
  }

  if (data.opening_hours !== null && data.opening_hours !== undefined && data.opening_hours != "NaN") {
    // just for checking opening hours
  } else {
    // just for checking opening hours
  }
  get_type_tags(data.types);
}


function handleMarkerClick(markerId) {
  const sidebarName = document.getElementById('name');
  const sidebarAddress = document.getElementById('address');
  const sidebarWebsiteUrl = document.getElementById('website-url');

  const apiUrl = `/api/restaurant/${markerId}`; // Korrekte URL mit markerId
  fetch(apiUrl)
    .then(response => response.json())
    .then(data => {
      if (data.error) {
        console.error('Fehler:', data.error);
      } else {
        get_details(data);
        get_star_rating(data);
        get_summaries(data); 

        const infoTab = document.querySelector('#nav-info-tab');
        const bootstrapTab = new bootstrap.Tab(infoTab);
        bootstrapTab.show();
      }
  })
  .catch(error => {
      console.error('Fehler beim Abrufen der Restaurant-Daten:', error);
    });
}










// all things filter
function update_filter_stars(rating, category) {
  const updateStars = (rating, group) => {
    const stars = [
      document.getElementById(`star-1-filter-${group}`),
      document.getElementById(`star-2-filter-${group}`),
      document.getElementById(`star-3-filter-${group}`),
      document.getElementById(`star-4-filter-${group}`),
      document.getElementById(`star-5-filter-${group}`),
    ];

    stars.forEach((star, index) => {
      if ((index+1) <= rating) {
        star.src = "static/graphics/voller_stern.png"; // Voller Stern
      } else {
        star.src = "static/graphics/leerer_stern.png"; // Leerer Stern
      }
    });
  };

  if (filters[category] == 0 || filters[category] != rating) {
    updateStars(rating, category);
    filters[category] = rating;
  } else {
    updateStars(0, category);
    filters[category] = 0;
  }
}


function update_filter_price_rating(button, range) {
  const price_buttons = [
    document.getElementById(`price-1-10`).getAttribute("data-active"),
    document.getElementById(`price-10-20`).getAttribute("data-active"),
    document.getElementById(`price-20-30`).getAttribute("data-active"),
    document.getElementById(`price-30-40`).getAttribute("data-active"),
    document.getElementById(`price-40-50`).getAttribute("data-active"),
    document.getElementById(`price-50-60`).getAttribute("data-active"),
    document.getElementById(`price-60-70`).getAttribute("data-active"),
    document.getElementById(`price-70-80`).getAttribute("data-active"),
    document.getElementById(`price-80-90`).getAttribute("data-active"),
    document.getElementById(`price-90-100`).getAttribute("data-active"),
    document.getElementById(`price-100+`).getAttribute("data-active"),
  ];

  const isActive = button.getAttribute("data-active") === "true";

  if (isActive) {
    button.setAttribute("data-active", "false");
    button.classList.remove("btn-price-filter-active");
    button.classList.add("btn-price-filter-inactive");

    // removing the type of the type list in the filters
    const index = filters.price.indexOf(range);
    if (index > -1) {
      filters.price.splice(index, 1); // Entfernt das Element aus der Liste
    }

  } else {
    button.setAttribute("data-active", "true");
    button.classList.remove("btn-price-filter-inactive");
    button.classList.add("btn-price-filter-active");

    if (!filters.price.includes(range)) {
      filters.price.push(range);
    }
  }
}


function display_food_type_buttons(types) {
  const container = document.getElementById("filter-food-types");   //container on the html in which the badges will go
  container.innerHTML = ""; //deleting everything in it

  for (const element of types) {
    // getting name without underscore and restaurant 
    let name = element.replace("_restaurant", "").replace(/_/g, " ");
    name = name.replace(/_/g, " ");
    // creating a new button element
    const button = document.createElement("span");
    button.className = "btn btn-price-filter-inactive mb-2 me-2"; // Bootstrap-Klassen
    button.setAttribute("data-active", "false");
    button.textContent = name; // Textinhalt setzen
    button.id = `type-${element}`;
    button.onclick = () => update_filter_type(button, element);


    // Badge dem Container hinzufügen
    container.appendChild(button);
  }
}


function update_filter_type (button, type) {
  const isActive = button.getAttribute("data-active") === "true";

  if (isActive) {
    // deactivating the button
    button.setAttribute("data-active", "false");
    button.classList.remove("btn-price-filter-active");
    button.classList.add("btn-price-filter-inactive");

    // removing the type of the type list in the filters
    const index = filters.type.indexOf(type);
    if (index > -1) {
      filters.type.splice(index, 1); // Entfernt das Element aus der Liste
    }
  } else {
    // activating the button 
    button.setAttribute("data-active", "true");
    button.classList.remove("btn-price-filter-inactive");
    button.classList.add("btn-price-filter-active");

    if (!filters.type.includes(type)) {
      filters.type.push(type);
    } 
  }
}




function delete_all_filters() {
  // deleting stars
  const updateStars = (group) => {
    const stars = [
      document.getElementById(`star-1-filter-${group}`),
      document.getElementById(`star-2-filter-${group}`),
      document.getElementById(`star-3-filter-${group}`),
      document.getElementById(`star-4-filter-${group}`),
      document.getElementById(`star-5-filter-${group}`),
    ];

    stars.forEach((star, index) => {
        star.src = "static/graphics/leerer_stern.png"; // Leerer Stern
      });

    filters[group] = 0;
    
  };
  updateStars("general");
  updateStars("food");
  updateStars("service");
  updateStars("atmosphere");

  // deleting price filters
  const price_buttons = [
    document.getElementById(`price-1-10`),
    document.getElementById(`price-10-20`),
    document.getElementById(`price-20-30`),
    document.getElementById(`price-30-40`),
    document.getElementById(`price-40-50`),
    document.getElementById(`price-50-60`),
    document.getElementById(`price-60-70`),
    document.getElementById(`price-70-80`),
    document.getElementById(`price-80-90`),
    document.getElementById(`price-90-100`),
    document.getElementById(`price-100+`),
  ];

  price_buttons.forEach((button, index) => {
    button.setAttribute("data-active", "false");
    button.classList.remove("btn-price-filter-active");
    button.classList.add("btn-price-filter-inactive"); 
  });

  filters.price = [];

  

  // deleting type buttons
  const container = document.getElementById("filter-food-types");  // Container mit den Buttons
  const buttons = container.querySelectorAll("span");  // Alle Buttons im Container finden

  buttons.forEach(button => {
    // Setze den Button auf "inactive"
    button.setAttribute("data-active", "false");
    button.classList.remove("btn-price-filter-active");
    button.classList.add("btn-price-filter-inactive");

    // Hole den entsprechenden type aus der ID des Buttons
    const type = button.id.replace("type-", "");

    // Lösche den type aus der filters.type Liste, falls er vorhanden ist
    const index = filters.type.indexOf(type);
    if (index > -1) {
      filters.type.splice(index, 1);  // Entfernt das Element aus der Liste
    }
  });


  // updating the map to show without filters
  update_map();
}


function filter() {
  update_map();
}












// All things graphs
function get_graph(data, type, category) {
  const filteredRows = JSON.parse(data);

  const svg = d3.select(`#graph-${type}-${category}`);
  const margin = { top: 20, right: 10, bottom: 70, left: 30 };

  function updateGraphSize() {
    // Berechne die Breite und Höhe des SVG-Containers dynamisch
    const containerWidth = svg.node().getBoundingClientRect().width || 400; // Fallback-Breite
    const containerHeight = svg.node().getBoundingClientRect().height || 300; // Fallback-Höhe

    const width = containerWidth - margin.left - margin.right;
    const height = containerHeight - margin.top - margin.bottom;

    // Setze die SVG-Dimensionen
    svg.attr("width", containerWidth).attr("height", containerHeight + 25);

    // Lösche vorherige Inhalte
    svg.selectAll("*").remove();

    // Überschrift hinzufügen
    let heading;
    if (category === 'food') {
      heading = type === 'avg' ? 'Average food stars per month' : 'Total amount food reviews per month';
    } else if (category === 'service') {
      heading = type === 'avg' ? 'Average service stars per month' : 'Total amount service reviews per month';
    } else if (category === 'atmosphere') {
      heading = type === 'avg' ? 'Average atmosphere stars per month' : 'Total amount atmosphere reviews per month';
    }

    svg.append("text")
      .attr("x", containerWidth / 2) // Horizontale Mitte
      .attr("y", margin.top) // Platz oberhalb des Graphen
      .attr("text-anchor", "middle") // Zentriere den Text
      .style("font-size", "16px") // Schriftgröße
      .style("font-weight", "bold") // Fettgedruckt
      .style("fill", "white") 
      .text(heading); // Text der Überschrift
  
    const chart = svg.append("g")
      .attr("transform", `translate(${margin.left},${margin.top + 25})`);

    const x = d3.scaleBand()
      .domain(filteredRows.map(d => d.review_month))
      .range([0, width])
      .padding(0.1);

    chart.append("g")
      .attr("transform", `translate(0, ${height})`)
      .call(d3.axisBottom(x))
      .selectAll("text")
      .attr("transform", "rotate(-45)")
      .style("text-anchor", "end");

    let dataField;
    const starType = type === 'avg' ? 'mean' : 'count';
    dataField = `dining_stars_${starType}`;

    const maxCount = d3.max(filteredRows, d => parseFloat(d[dataField]));
    const y = d3.scaleLinear()
      .domain([0, maxCount || 1]) // Fallback-Wert
      .range([height, 0]);

    chart.append("g").call(d3.axisLeft(y));

    const line = d3.line()
      .x(d => x(d.review_month) + x.bandwidth() / 2)
      .y(d => y(d[dataField]));

    chart.append("path")
      .datum(filteredRows)
      .attr("fill", "none")
      .attr("stroke", "#F49069")
      .attr("stroke-width", 2)
      .attr("d", line);

    chart.selectAll("circle")
      .data(filteredRows)
      .join("circle")
      .attr("cx", d => x(d.review_month) + x.bandwidth() / 2)
      .attr("cy", d => y(d[dataField]))
      .attr("r", 5)
      .attr("fill", "#F49069");
  }

  updateGraphSize();
  window.addEventListener("resize", updateGraphSize);
}



function get_graph_price(data) {
  const filteredRows = JSON.parse(data);
  filteredRows.sort((a, b) => d3.ascending(a.review_month, b.review_month));

  // SVG-Setup: Wählen des SVG-Elements
  const svg = d3.select("#graph-total-price"); // Dein SVG für den Bar Plot
  const margin = { top: 20, right: 30, bottom: 200, left: 40 };

  // Berechne die Breite und Höhe dynamisch
  function updateGraphSize() {
    const containerWidth = svg.node().getBoundingClientRect().width || 400;
    const containerHeight = svg.node().getBoundingClientRect().height || 250;

    // Setze die neuen Dimensionen des SVG-Elements
    svg.attr("width", containerWidth + margin.left + margin.right);
    svg.attr("height", containerHeight + margin.top + margin.bottom);

    // Lösche bestehende Inhalte (falls das Graph neu gezeichnet werden soll)
    svg.selectAll("*").remove();

    const chart = svg.append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    // X-Achse (Preisbereich)
    console.log(filteredRows.map(d => d.dining_price_range)); 

    const x = d3.scaleBand()
      .domain(filteredRows.map(d => d.dining_price_range)) // Wähle dining_price_range für die X-Achse
      .range([0, containerWidth])
      .padding(0.1);

    chart.append("g")
      .attr("transform", `translate(0, ${containerHeight} + 30)`)
      .call(d3.axisBottom(x))
      .selectAll("text")
      .attr("transform", "rotate(-45)")
      .style("text-anchor", "end")
      .style("font-size", "12px")
      .style("fill", "white");;

    // Y-Achse (Anzahl der Bewertungen)
    const maxCount = d3.max(filteredRows, d => parseFloat(d.dining_price_range_count));
    const y = d3.scaleLinear()
      .domain([0, maxCount]) // Höchster Wert der dining_price_range_count
      .nice() // Runden der Werte für die Achse
      .range([containerHeight, 0]);

    chart.append("g")
      .call(d3.axisLeft(y));

    // Erstelle die Bars
    chart.selectAll(".bar")
      .data(filteredRows)
      .enter().append("rect")
      .attr("class", "bar")
      .attr("x", d => x(d.dining_price_range))
      .attr("y", d => y(d.dining_price_range_count))
      .attr("width", x.bandwidth())
      .attr("height", d => containerHeight - y(d.dining_price_range_count))
      .attr("fill", "#F49069");


    // Füge Überschrift hinzu
    const heading = "Price Range Distribution"; // Beispiel Überschrift
    svg.append("text")
      .attr("x", containerWidth / 2) // Horizontale Mitte
      .attr("y", margin.top) // Platz oberhalb des Graphen
      .attr("text-anchor", "middle") // Zentriere den Text
      .style("font-size", "16px") // Schriftgröße
      .style("font-weight", "bold") // Fettgedruckt
      .style("fill", "white") // Textfarbe
      .text(heading);
  }

  // Initialisiere den Graphen
  updateGraphSize();

  // Aktualisiere den Graphen bei einer Größenänderung des Fensters
  window.addEventListener("resize", updateGraphSize);
}








// only when clicking on the tab, the size will be rendered, so when tab is clicked function to construct graph will be opened
document.querySelector('#nav-food-tab').addEventListener('shown.bs.tab', () => {
  if (typeof activeMarker !== 'undefined' && activeMarker !== null) {
    const markerId = activeMarker.options.id; // Zugriff auf die Marker-ID
    const category = 'food';

    // getting price range distribution data from the database
    const apiUrl = `/api/avg_${category}/${markerId}`; // Korrekte URL mit markerId
      fetch(apiUrl)
        .then(response => response.json())
        .then(data => {
          if (data.error) {
            console.error('Fehler:', data.error);
          } else {
            get_graph(data, "avg", category);
            get_graph(data, "total", category);
          }
      })
    .catch(error => {
        console.error('Fehler beim Abrufen der Restaurant-Daten:', error);
      });

  
  } else {
    console.error('Kein aktiver Marker gefunden!');
  }
});


document.querySelector('#nav-service-tab').addEventListener('shown.bs.tab', () => {
  if (typeof activeMarker !== 'undefined' && activeMarker !== null) {
    const markerId = activeMarker.options.id; // Zugriff auf die Marker-ID
    const category = 'service';

    // getting price range distribution data from the database
    const apiUrl = `/api/avg_${category}/${markerId}`; // Korrekte URL mit markerId
      fetch(apiUrl)
        .then(response => response.json())
        .then(data => {
          if (data.error) {
            console.error('Fehler:', data.error);
          } else {
            get_graph(data, "avg", category);
            get_graph(data, "total", category);
          }
      })
    .catch(error => {
        console.error('Fehler beim Abrufen der Restaurant-Daten:', error);
      });

    
  } else {
    console.error('Kein aktiver Marker gefunden!');
  }
});

document.querySelector('#nav-atmosphere-tab').addEventListener('shown.bs.tab', () => {
  if (typeof activeMarker !== 'undefined' && activeMarker !== null) {
    const markerId = activeMarker.options.id; // Zugriff auf die Marker-ID
    const category = 'atmosphere';

    // getting price range distribution data from the database
    const apiUrl = `/api/avg_${category}/${markerId}`; // Korrekte URL mit markerId
      fetch(apiUrl)
        .then(response => response.json())
        .then(data => {
          if (data.error) {
            console.error('Fehler:', data.error);
          } else {
            get_graph(data, "avg", category);
            get_graph(data, "total", category);
          }
      })
    .catch(error => {
        console.error('Fehler beim Abrufen der Restaurant-Daten:', error);
      });

  } else {
    console.error('Kein aktiver Marker gefunden!');
  }
});

document.querySelector('#nav-price-tab').addEventListener('shown.bs.tab', () => {
  if (typeof activeMarker !== 'undefined' && activeMarker !== null) {
    const markerId = activeMarker.options.id; // Zugriff auf die Marker-ID

    // getting price range distribution data from the database
    const apiUrl = `/api/avg_price/${markerId}`; // Korrekte URL mit markerId
      fetch(apiUrl)
        .then(response => response.json())
        .then(data => {
          if (data.error) {
            console.error('Fehler:', data.error);
          } else {
            // checking if there is at least one review with a price range
            const parsedData = JSON.parse(data);
            const hasNonZeroCount = parsedData.some(item => item.dining_price_range_count > 0);

            if (hasNonZeroCount) {
              get_graph_price(data);
            } else {
              console.log("No review included a price range.");
            }

            // const infoTab = document.querySelector('#nav-info-tab');
            // const bootstrapTab = new bootstrap.Tab(infoTab);
            // bootstrapTab.show();
          }
      })
  .catch(error => {
      console.error('Fehler beim Abrufen der Restaurant-Daten:', error);
    });
    
  } else {
    console.error('Kein aktiver Marker gefunden!');
  }
});