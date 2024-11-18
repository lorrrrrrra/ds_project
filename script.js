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


function open_details() {
  const sidebar = document.querySelector('.details-sidebar');
  
  if (!sidebar.classList.contains('open')) {
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