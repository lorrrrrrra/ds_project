// loading of the map
const map = L.map('map').setView([51.505, -0.09], 13);
    
const tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);

// new Icon for clicked Marker
var defaultIcon = L.icon({
  iconUrl: 'marker_green.png', // Standardmarker
  iconSize: [37.5, 58],
  iconAnchor: [22, 94],
  shadowAnchor: [4, 62],
  popupAnchor: [-3, -76],
});

var clickedIcon = L.icon({
  iconUrl: 'marker_orange.png', // Marker bei Klick
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

// loading the general data for the restaurants
fetch('API_general.csv')
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


fetch('csv files/reviews_grouped.csv')
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


fetch('csv files/reviews_grouped_month.csv')
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


fetch('csv files/reviews_grouped_year.csv')
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


fetch('csv files/dining_price_range_group.csv')
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


fetch('csv files/filtered_summary_restaurants.csv')
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


fetch('API_basics.csv')
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
                handleMarkerClick(e.target.options.id); // ID des Markers verwenden
            });

            if (restaurant === restaurants[0]) {
              activeMarker = marker; // Setze den ersten Marker als aktiven Marker
              marker.setIcon(clickedIcon); // Setze das Icon des aktiven Markers auf clickedIcon
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
          handleMarkerClick(id);
        }
      },
      error: (error) => console.error('Fehler beim Parsen der CSV:', error),
    });
  })
  .catch((error) => console.error('Fehler beim Laden der CSV:', error));





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
    //updateAverageRating(star_rating_food, '-food-ov'); // Food-Bewertung
    //updateAverageRating(star_rating_service, '-service-ov'); // Service-Bewertung
    //updateAverageRating(star_rating_atmosphere, '-atmosphere-ov'); // Atmosphere-Bewertung
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
        const hasHalfStar = (rating % 1) >= 0.3 && (rating % 1) < 0.75;
        const hasFullStar = (rating % 1) >= 0.75;
  
        stars.forEach((star, index) => {
          if (index < fullStars) {
            star.src = "/graphics/voller_stern.png"; // Voller Stern
          } else if (index === fullStars && hasHalfStar) {
            star.src = "/graphics/halber_stern.png"; // Halber Stern
          } else if (index === fullStars && hasFullStar) {
            star.src = "/graphics/voller_stern.png"; // Voller Stern
          } else {
            star.src = "/graphics/leerer_stern.png"; // Leerer Stern
          }
        });
      };
  
      // Sterne für jede Gruppe aktualisieren
      updateStars(star_rating, ''); // Allgemeine Sterne
      //updateStars(star_rating_food, '-food-ov'); // Food
      //updateStars(star_rating_service, '-service-ov'); // Service
      //updateStars(star_rating_atmosphere, '-atmosphere-ov'); // Atmosphere
      updateStars(star_rating_food, '-food'); // Food
      updateStars(star_rating_service, '-service'); // Service
      updateStars(star_rating_atmosphere, '-atmosphere'); // Atmosphere
  
      console.log({
        general: star_rating,
        food: star_rating_food,
        service: star_rating_service,
        atmosphere: star_rating_atmosphere,
      });
    } else {
      console.error(`Kein Restaurant mit der ID ${markerId} gefunden.`);
    }
  }




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
  
      const y = d3.scaleLinear()
        .domain([0, d3.max(filteredRows, d => d[dataField]) || 1]) // Fallback-Wert
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




function handleMarkerClick(markerId) {
  // Restaurant mit passender ID suchen
  const restaurant = restaurants.find((r) => r.restaurant_id === markerId);
  
  if (restaurant) {
    const name = restaurant.name || 'Unbekanntes Restaurant';
    const address = restaurant.address || 'Keine Adresse verfügbar';
  
    // Dynamische Aktualisierung der Sidebar mit den Details
    const sidebarName = document.getElementById('name');
    const sidebarAddress = document.getElementById('address');
    // const starRating = document.getElementById('star-rating');

    getStarRating(markerId);
  
    if (sidebarName && sidebarAddress) {
      sidebarName.textContent = name;
      sidebarAddress.textContent = address;

    const infoTab = document.querySelector('#nav-info-tab');
    const bootstrapTab = new bootstrap.Tab(infoTab);
    bootstrapTab.show();

    }
  } else {
    console.error(`Kein Restaurant mit der ID ${markerId} gefunden.`);
  }
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