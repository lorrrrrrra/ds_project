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




  function get_graph(markerId,) {
    const filteredRows = reviews_grouped_month.filter((r) => r.restaurant_id === markerId);
  
    // SVG-Setup: Wählen eines SVG-Elements mit einer bestimmten ID
    const svg = d3.select("#graph-avg-food"); // Ersetze "your-svg-id" mit der tatsächlichen ID des SVG-Elements
    const margin = { top: 20, right: 30, bottom: 50, left: 50 };
  
    // Berechne die Breite und Höhe dynamisch
    function updateGraphSize() {
      const width = svg.node().getBoundingClientRect().width - margin.left - margin.right; // Dynamische Breite des Containers
      const fullHeight = svg.attr("height") - margin.top - margin.bottom;
      const height = fullHeight / 2;
  
      // Setze die neuen Dimensionen
      svg.attr("width", width + margin.left + margin.right); // Um sicherzustellen, dass es 100% der Containerbreite einnimmt
  
      // Lösche bestehende Inhalte (falls das Graph neu gezeichnet werden soll)
      svg.selectAll("*").remove();
  
      const chart = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`);
  
      // X-Achse (Zeitachse)
      const x = d3.scaleBand()
        .domain(filteredRows.map(d => d.review_month)) // Monatsnamen als X-Achse
        .range([0, width])
        .padding(0.1);
  
      chart.append("g")
        .attr("transform", `translate(0, ${height})`)
        .call(d3.axisBottom(x))
        .selectAll("text")
        .attr("transform", "rotate(-45)")
        .style("text-anchor", "end");
  
      
      // Y-Achse (Bewertungen)
      const y = d3.scaleLinear()
        .domain([0, d3.max(filteredRows, d => d.dining_stars_food_mean)]) // Wertebereich von 0 bis max
        .range([height, 0]);
  
      const tickValues = [];
      const tickStep = Math.floor(d3.max(filteredRows, d => d.dining_stars_food_mean) / 5); // Schrittweite für die Ticks

      for (let i = 0; i <= d3.max(filteredRows, d => d.dining_stars_food_mean); i += tickStep) {
        tickValues.push(i);
      }

      chart.append("g")
      .call(d3.axisLeft(y)
        .tickValues(tickValues) // Setze nur alle zwei Abschnitte Ticks
        .tickSize(-width) // Horizontale Striche im Hintergrund
        .tickPadding(10)) // Abstand der Ticks vom Rand der Achse
      .selectAll("line")
      .attr("stroke", "#ccc"); // Farbe der Striche im Hintergrund
  
      // Linie ohne Kurve (gerade Linien)
      const line = d3.line()
        .x(d => x(d.review_month) + x.bandwidth() / 2) // Mittelpunkt der Kategorie
        .y(d => y(d.dining_stars_food_mean));
  
      chart.append("path")
        .datum(filteredRows)
        .attr("fill", "none")
        .attr("stroke", "#F49069")
        .attr("stroke-width", 2)
        .attr("d", line);
  
      // Punkte markieren
      chart.selectAll("circle")
        .data(filteredRows)
        .join("circle")
        .attr("cx", d => x(d.review_month) + x.bandwidth() / 2)
        .attr("cy", d => y(d.dining_stars_food_mean))
        .attr("r", 5)
        .attr("fill", "#F49069");
    }
  
    // Initialisiere den Graphen mit der aktuellen Größe
    window.onload = updateGraphSize();
  
    // Aktualisiere den Graphen bei einer Größenänderung des Fensters
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
    get_graph(markerId);
  
    if (sidebarName && sidebarAddress) {
      sidebarName.textContent = name;
      sidebarAddress.textContent = address;

    }
  } else {
    console.error(`Kein Restaurant mit der ID ${markerId} gefunden.`);
  }
}


