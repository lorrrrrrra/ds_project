// loading of the map
const map = L.map('map').setView([51.505, -0.09], 13);
    
const tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);

// new Icon for clicked Marker
var defaultIcon = L.icon({
  iconUrl: 'marker_green.png', // Standardmarker
  iconSize: [37.5, 61.5],
  iconAnchor: [22, 94],
  shadowAnchor: [4, 62],
  popupAnchor: [-3, -76],
});

var clickedIcon = L.icon({
  iconUrl: 'marker_orange.png', // Marker bei Klick
  iconSize: [37.5, 61.5],
  iconAnchor: [22, 94],
  shadowAnchor: [4, 62],
  popupAnchor: [-3, -76],
});


        

let restaurants = []; // Globale Variable
let activeMarker = null;

fetch('API_basics.csv')
  .then((response) => response.text())
  .then((csvData) => {
    // CSV parsen
    Papa.parse(csvData, {
      header: true, // Erste Zeile als Header interpretieren
      complete: (results) => {
        restaurants = results.data; // Speichern der geparsten Restaurants

        // Nur die ersten 10 Restaurants anzeigen
        const first10Restaurants = restaurants.slice(0, 10);

        first10Restaurants.forEach((restaurant) => {
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

            if (restaurant === first10Restaurants[0]) {
              activeMarker = marker; // Setze den ersten Marker als aktiven Marker
              marker.setIcon(clickedIcon); // Setze das Icon des aktiven Markers auf clickedIcon
              handleMarkerClick(id); // Führe die Funktion für den ersten Marker aus
            }

          } else {
            console.error('Ungültige Koordinaten:', restaurant);
          }
        });

        // Optional: Karte auf das erste Restaurant fokussieren
        if (first10Restaurants.length > 0) {
          const firstRestaurant = first10Restaurants[0];
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




  function handleMarkerClick(markerId) {
    // Restaurant mit passender ID suchen
    const restaurant = restaurants.find((r) => r.restaurant_id === markerId);
  
    if (restaurant) {
      const name = restaurant.name || 'Unbekanntes Restaurant';
      const address = restaurant.address || 'Keine Adresse verfügbar';
  
      // Dynamische Aktualisierung der Sidebar mit den Details
      const sidebarName = document.getElementById('name');
      const sidebarAddress = document.getElementById('address');
  
      if (sidebarName && sidebarAddress) {
        sidebarName.textContent = name;
        sidebarAddress.textContent = address;
      }
    } else {
      console.error(`Kein Restaurant mit der ID ${markerId} gefunden.`);
    }
  }