function openLoggingSidebar() {
  const html = HtmlService.createTemplateFromFile('sidebar/sidebar').evaluate()
    .setTitle('Tiller Sidebar')
    .setWidth(300);
  SpreadsheetApp.getUi().showSidebar(html);
}
function onOpen() {
  var ui = SpreadsheetApp.getUi();
  // Create a custom menu in the Google Sheets UI.
  ui.createMenu('Tiller Sidebar')
      .addItem('Open...', 'openLoggingSidebar')
      .addToUi();
}
