function openLoggingSidebar() {
  const html = HtmlService.createTemplateFromFile('sidebar').evaluate()
    .setTitle('Logging Sidebar')
    .setWidth(300);
  SpreadsheetApp.getUi().showSidebar(html);
}
function onOpen() {
  var ui = SpreadsheetApp.getUi();
  // Create a custom menu in the Google Sheets UI.
  ui.createMenu('Logger')
      .addItem('Launch', 'openLoggingSidebar')
      .addToUi();
}
