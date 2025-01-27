// loading of the map
const map = L.map('map').setView([51.505, -0.09], 13);
    
const tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);


// new Icon for clicked Marker
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


    




let restaurants = []; // Globale Variable
let restaurants_general = [];
let reviews_grouped = [];
let reviews_grouped_month = [] ;
let reviews_grouped_year = [];
let reviews_grouped_price = [];
let summaries = [];
let activeMarker = null;

let filters = {
  general: 0,
  food: 0,
  service: 0,
  atmosphere: 0,
  price_filter: false,
  price_0_10: false,
  price_10_20: false,
  price_20_30: false,
  price_30_40: false,
  price_40_50: false,
  price_50_100: false
}

let types = ["african_restaurant", "asian_restaurant", "bakery", "bar", "breakfast_restaurant", "brunch_restaurant", 
  "buffet_restaurant", "cafe", "chinese_restaurant", "fast_food_restaurant", "fine_dining_restaurant", "indian_restaurant", 
  "italian_restaurant", "meal_delivery", "meal_takeaway", "seafood_restaurant", "sushi_restaurant", "vegan_restaurant", 
  "vegetarian_restaurant" ]

//initiate filter buttons for types
display_food_type_buttons(types);


// function fetchRestaurantData(Url) {
//   fetch(Url)
//       .then(response => response.json())  // Umwandeln der Antwort in JSON
//       .then(data => {
//           console.log(data);  // Die Daten im Console-Log anzeigen
//           // Hier kannst du die Daten verwenden, um sie im Frontend darzustellen
//           // Zum Beispiel könnte man die Daten in einer Tabelle anzeigen
//       })
//       .catch(error => {
//           console.error('Fehler beim Abrufen der Daten:', error);
//       });
// }

// Die Funktion aufrufen, um die Daten zu laden
// fetchRestaurantData(apiUrl);
// fetchRestaurantData(apiUrl_2);



// loading the general data for the restaurants
fetch('static/API_general.csv')
.then((response) => response.text())
.then((csvData) => {
  // CSV parsen
  Papa.parse(csvData, {
    header: true, // Erste Zeile als Header interpretieren
    complete: (results) => {
      restaurants_general = results.data; // Speichern der geparsten Restaurants
    },
    error: (error) => console.error('Fehler beim Parsen der CSV:', error),
  });
})
.catch((error) => console.error('Fehler beim Laden der CSV:', error));


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


fetch('static/csv files/filtered_summary_restaurants.csv')
.then((response) => response.text())
.then((csvData) => {
  // CSV parsen
  Papa.parse(csvData, {
    header: true, // Erste Zeile als Header interpretieren
    complete: (results) => {
      summaries = results.data; // Speichern der geparsten Restaurants
    },
    error: (error) => console.error('Fehler beim Parsen der CSV:', error),
  });
})
.catch((error) => console.error('Fehler beim Laden der CSV:', error));


// Funktion zum Schließen des Stripes
function close_sidebar() {
  const rightSidebar = document.getElementById('rightSidebar');
  rightSidebar.classList.add('closed');
  // rightSidebar.style.width = '0'; // Breite auf 0 setzen, um den Stripe zu schließen
  // rightSidebar.style.minWidth = '0'; // Minimum Breite ebenfalls anpassen
}

// Funktion zum Öffnen des Stripes
function open_sidebar() {
  const rightSidebar = document.getElementById('rightSidebar');
  rightSidebar.classList.remove('closed'); // Sidebar öffnen
  // rightSidebar.style.width = '30%'; // Breite auf Standardwert setzen
  // rightSidebar.style.minWidth = '70px'; // Minimum Breite zurücksetzen
}





fetch('static/API_basics.csv')
  .then((response) => response.text())
  .then((csvData) => {
    // CSV parsen
    Papa.parse(csvData, {
      header: true, // Erste Zeile als Header interpretieren
      complete: (results) => {
        restaurants = results.data; // Speichern der geparsten Restaurants

        // Nur die ersten 10 Restaurants anzeigen

        restaurants.forEach((restaurant) => {
          const lat = parseFloat(restaurant.lat_value);
          const lon = parseFloat(restaurant.long_value);
          const id = restaurant.restaurant_id;

          // Marker setzen
          if (!isNaN(lat) && !isNaN(lon)) {
            const marker = L.marker([lat, lon], { id: id, icon: defaultIcon})
              .addTo(map)
            
              marker.on('click', (e) => {
                if (activeMarker) {
                  activeMarker.setIcon(defaultIcon);
                }
                activeMarker = marker;
                marker.setIcon(clickedIcon);
                open_sidebar();
                handleMarkerClick(e.target.options.id); // ID des Markers verwenden
            });

            if (restaurant === restaurants[0]) {
              activeMarker = marker; // Setze den ersten Marker als aktiven Marker
              marker.setIcon(clickedIcon); // Setze das Icon des aktiven Markers auf clickedIcon
              open_sidebar();
              handleMarkerClick(id); // Führe die Funktion für den ersten Marker aus
            }

          } else {
            console.error('Ungültige Koordinaten:', restaurant);
          }
        });

        // Optional: Karte auf das erste Restaurant fokussieren
        if (restaurants.length > 0) {
          const firstRestaurant = restaurants[0];
          const lat = parseFloat(firstRestaurant.lat_value);
          const lon = parseFloat(firstRestaurant.long_value);
          const id = firstRestaurant.restaurant_id;
          map.setView([lat, lon], 15);
          open_sidebar();
          handleMarkerClick(id);
        }
      },
      error: (error) => console.error('Fehler beim Parsen der CSV:', error),
    });
  })
  .catch((error) => console.error('Fehler beim Laden der CSV:', error));








  // all things details side
  function getStarRating(markerId) {
    const restaurant = reviews_grouped.find((r) => r.restaurant_id === markerId);
  
    if (restaurant) {
      // Allgemeine Sternebewertung
      const star_rating = parseFloat(restaurant.stars_mean);
      const star_rating_food = parseFloat(restaurant.dining_stars_food_mean);
      const star_rating_service = parseFloat(restaurant.dining_stars_service_mean);
      const star_rating_atmosphere = parseFloat(restaurant.dining_stars_atmosphere_mean);
  
      // Durchschnittliche Bewertungen aktualisieren
    const updateAverageRating = (rating, group) => {
      const avgRatingElement = document.getElementById(`avg-rating${group}`);
      if (rating != null) {
        avgRatingElement.textContent = ` (${rating.toFixed(1)})`;
      } else {
        avgRatingElement.textContent = ` `;
      }
    };

    updateAverageRating(star_rating, ''); // Allgemeine Bewertung
    // updateAverageRating(star_rating_food, '-food-detail'); // Food-Bewertung
    // updateAverageRating(star_rating_service, '-service-detail'); // Service-Bewertung
    // updateAverageRating(star_rating_atmosphere, '-atmosphere-detail'); // Atmosphere-Bewertung
    updateAverageRating(star_rating_food, '-food'); // Food-Bewertung
    updateAverageRating(star_rating_service, '-service'); // Service-Bewertung
    updateAverageRating(star_rating_atmosphere, '-atmosphere'); // Atmosphere-Bewertung
  
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
      console.error(`Kein Restaurant mit der ID ${markerId} gefunden.`);
    }
  }


function get_summaries(markerId) {
  // Restaurant mit passender ID suchen
  const restaurant = summaries.find((r) => r.restaurant_id === markerId);

  if (restaurant) {
    const summary_overall = restaurant.overall_summary;
    const summary_food = restaurant.food_summary;
    const summary_service = restaurant.service_summary;
    const summary_atmosphere = restaurant.atmosphere_summary;
    const summary_price = restaurant.price_summary;
    const count_overall = parseFloat(restaurant.overall_count);
    const count_food = parseFloat(restaurant.food_count);
    const count_service = parseFloat(restaurant.service_count);
    const count_atmosphere = parseFloat(restaurant.atmosphere_count);
    const count_price = parseFloat(restaurant.price_count);

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
    html_summary_food.textContent = summary_food;
    html_summary_service.textContent = summary_service;
    html_summary_atmosphere.textContent = summary_atmosphere;
    html_summary_price.textContent = summary_price;

    label_amount_overall.textContent = `generated with AI - based on ${count_overall} reviews`;
    label_amount_food.textContent = `generated with AI -based on ${count_food} reviews`;
    label_amount_service.textContent = `generated with AI -based on ${count_service} reviews`;
    label_amount_atmosphere.textContent = `generated with AI -based on ${count_atmosphere} reviews`;
    label_amount_price.textContent = `generated with AI -based on ${count_price} reviews`;
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
        console.log('Restaurant-Daten:', data);
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
    })
    .catch(error => {
      console.error('Fehler beim Abrufen der Restaurant-Daten:', error);
    });

    // Restaurant mit passender ID suchen
  const restaurant = restaurants.find((r) => r.restaurant_id === markerId);
  
  if (restaurant) {
    getStarRating(markerId);
    get_summaries(markerId);
  
    if (sidebarName && sidebarAddress) {
      // sidebarName.textContent = name;
      // sidebarAddress.textContent = address;

      const infoTab = document.querySelector('#nav-info-tab');
      const bootstrapTab = new bootstrap.Tab(infoTab);
      bootstrapTab.show();

    }
  } else {
    console.error(`Kein Restaurant mit der ID ${markerId} gefunden.`);
  }
}










// all things filter
function set_filter_stars(rating, category) {
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


function update_price_rating(button, min, max) {
  const price_buttons = [
    document.getElementById(`price-0-10`).getAttribute("data-active"),
    document.getElementById(`price-10-20`).getAttribute("data-active"),
    document.getElementById(`price-20-30`).getAttribute("data-active"),
    document.getElementById(`price-30-40`).getAttribute("data-active"),
    document.getElementById(`price-40-50`).getAttribute("data-active"),
    document.getElementById(`price-50-100`).getAttribute("data-active"),
  ];

  const isActive = button.getAttribute("data-active") === "true";

  if (isActive) {
    button.setAttribute("data-active", "false");
    button.classList.remove("btn-price-filter-active");
    button.classList.add("btn-price-filter-inactive");
    let falseCount = price_buttons.filter(value => value === "false").length;

    let price_category = `price_${min}_${max}`;
    filters[price_category] = false;

    if (falseCount === 4) {
      filters["price_filter"] = false;
    }
  } else {
    button.setAttribute("data-active", "true");
    button.classList.remove("btn-price-filter-inactive");
    button.classList.add("btn-price-filter-active");
    let price_category = `price_${min}_${max}`;
    filters["price_filter"] = true;
    filters[price_category] = true;  
  }
}


function display_food_type_buttons(types) {
  const container = document.getElementById("filter-food-types");   //container on the html in which the badges will go
  container.innerHTML = ""; //deleting everything in it

  for (const element of types) {
    // getting name without underscore and restaurant 
    let name = element.replace("_restaurant", "");
    name = name.replace(/_/g, " ");
    // creating a new button element
    const button = document.createElement("span");
    button.className = "btn btn-price-filter-inactive mb-2"; // Bootstrap-Klassen
    button.setAttribute("data-active", "false");
    button.textContent = name; // Textinhalt setzen


    // Badge dem Container hinzufügen
    container.appendChild(button);
  }
}




function delete_all_filters() {
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

  const price_buttons = [
    document.getElementById(`price-0-10`),
    document.getElementById(`price-10-20`),
    document.getElementById(`price-20-30`),
    document.getElementById(`price-30-40`),
    document.getElementById(`price-40-50`),
    document.getElementById(`price-50-100`),
  ];

  price_buttons.forEach((button, index) => {
    button.setAttribute("data-active", "false");
    button.classList.remove("btn-price-filter-active");
    button.classList.add("btn-price-filter-inactive"); 
  });

  filters["price_filter"] = false;
  filters["price_0_10"] = false; 
  filters["price_10_20"] = false; 
  filters["price_20_30"] = false; 
  filters["price_30_40"] = false; 
  filters["price_40_50"] = false; 
  filters["price_50_100"] = false;
}















// All things graphs
function get_graph(markerId, type, category) {
  const filteredRows = reviews_grouped_month.filter((r) => r.restaurant_id === markerId);

  const svg = d3.select(`#graph-${type}-${category}`);
  const margin = { top: 10, right: 10, bottom: 70, left: 30 };

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
    if (category === 'food') {
      dataField = type === 'avg' ? 'dining_stars_food_mean' : 'dining_stars_food_count';
    } else if (category === 'service') {
      dataField = type === 'avg' ? 'dining_stars_service_mean' : 'dining_stars_service_count';
    } else if (category === 'atmosphere') {
      dataField = type === 'avg' ? 'dining_stars_atmosphere_mean' : 'dining_stars_atmosphere_count';
    }

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



function get_graph_price(markerId) {
  const filteredRows = reviews_grouped_price.filter((r) => r.restaurant_id === markerId);

  // SVG-Setup: Wählen des SVG-Elements
  const svg = d3.select("#graph-total-price"); // Dein SVG für den Bar Plot
  const margin = { top: 20, right: 30, bottom: 70, left: 40 };

  // Berechne die Breite und Höhe dynamisch
  function updateGraphSize() {
    const containerWidth = svg.node().getBoundingClientRect().width || 400;
    const containerHeight = svg.node().getBoundingClientRect().height || 300;

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
      .attr("transform", `translate(0, ${containerHeight})`)
      .call(d3.axisBottom(x))
      .selectAll("text")
      .attr("transform", "rotate(-45)")
      .style("text-anchor", "end");

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
    const category = 'food'; // Beispielwert

    get_graph(markerId, "avg", category);
    get_graph(markerId, "total", category);
  } else {
    console.error('Kein aktiver Marker gefunden!');
  }
});


document.querySelector('#nav-service-tab').addEventListener('shown.bs.tab', () => {
  if (typeof activeMarker !== 'undefined' && activeMarker !== null) {
    const markerId = activeMarker.options.id; // Zugriff auf die Marker-ID
    const category = 'service';

    get_graph(markerId, "avg", category);
    get_graph(markerId, "total", category);
  } else {
    console.error('Kein aktiver Marker gefunden!');
  }
});

document.querySelector('#nav-atmosphere-tab').addEventListener('shown.bs.tab', () => {
  if (typeof activeMarker !== 'undefined' && activeMarker !== null) {
    const markerId = activeMarker.options.id; // Zugriff auf die Marker-ID
    const category = 'atmosphere';

    get_graph(markerId, "avg", category);
    get_graph(markerId, "total", category);
  } else {
    console.error('Kein aktiver Marker gefunden!');
  }
});

document.querySelector('#nav-price-tab').addEventListener('shown.bs.tab', () => {
  if (typeof activeMarker !== 'undefined' && activeMarker !== null) {
    const markerId = activeMarker.options.id; // Zugriff auf die Marker-ID

    get_graph_price(markerId);
  } else {
    console.error('Kein aktiver Marker gefunden!');
  }
});