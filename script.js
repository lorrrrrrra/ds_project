function toggleSidebar() {
  const sidebar = document.querySelector('.sidebar');
  const arrow = document.querySelector('.toggle-arrow');
  const content = document.querySelector('.dashboard-filter');
  const burgerMenu = document.getElementById('burger-menu');

  if (sidebar.style.width === '50px') {
      // Expand the sidebar
      sidebar.style.width = '20%';
      content.classList.remove('hidden'); // Inhalt wieder anzeigen
      arrow.textContent = '←'; // Pfeil zeigt nach links
      burgerMenu.style.display = 'none'; // Burger-Menü ausblenden
  } else {
      // Collapse the sidebar
      sidebar.style.width = '50px';
      content.classList.add('hidden'); // Inhalt verstecken
      arrow.textContent = '→'; // Pfeil zeigt nach rechts
      burgerMenu.style.display = 'block'; // Burger-Menü anzeigen
  }
}