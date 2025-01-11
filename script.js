let csvData = []; // Array to store the CSV data

// Load CSV data when the page loads
document.addEventListener('DOMContentLoaded', () => {
    fetch('restaurant_ratings_test.csv')
        .then(response => response.text())
        .then(data => {
            csvData = parseCSV(data); // Parse CSV data
            console.log('Loaded CSV Data:', csvData); // Debug: Zeigt die geladenen CSV-Daten
        })
        .catch(error => console.error('Error loading CSV:', error));
});

// Parse CSV data into an array of objects
function parseCSV(csvText) {
  const rows = csvText.trim().split('\n');
  const headers = rows.shift().split(',');
  return rows.map(row => {
      const values = row.split(',');
      return headers.reduce((acc, header, i) => {
          acc[header] = values[i];
          return acc;
      }, {});
  });
}


function toggleSidebar(instruction) {
  const sidebar = document.querySelector('.sidebar');
  const arrow = document.querySelector('.toggle-arrow');
  const content = document.querySelector('.dashboard-filter');
  const burgerMenu = document.getElementById('burger-menu');

  if (instruction == 'open') {
      // Expand the sidebar
      sidebar.style.width = '20%';
      content.classList.remove('hidden'); // Inhalt wieder anzeigen
      burgerMenu.style.display = 'none'; // Burger-Menü ausblenden
  } else if (instruction == 'close') { 
      // Collapse the sidebar
      sidebar.style.width = '50px';
      content.classList.add('hidden'); // Inhalt verstecken
      burgerMenu.style.display = 'block'; // Burger-Menü anzeigen
  }
}


function open_details(id) {
  const sidebar = document.querySelector('.details-sidebar');
  const selectedData = csvData.find(row => row.id == id); // Find the matching row by ID

    if (!sidebar.classList.contains('open')) {
      // show sidebar 
      sidebar.classList.add('open'); // Sidebar einblenden
      toggleSidebar('close');
    }
}


function close_details() {
  const sidebar = document.querySelector('.details-sidebar');

  if (sidebar.classList.contains('open')) {
    sidebar.classList.remove('open'); // Sidebar ausblenden
}
}