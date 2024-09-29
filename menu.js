function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Accounting')
    .addItem('Find Receipts', 'openReceiptFinderSidebar')
    .addToUi();
  ui.createMenu('Stripe')
    .addItem('Process Payout Data', 'processStripePayouts')
    .addItem('Configure Stripe Settings', 'openConfigDialog')
    .addToUi();
}

function openReceiptFinderSidebar() {
  const html = HtmlService.createTemplateFromFile('Sidebar-orig')
    .evaluate()
    .setTitle('Find Receipts')
    .setWidth(400);

  const toReturn = html.getContent() ; // .replaceAll("&lt;", "<").replaceAll("&gt;" , ">") ;

  Logger.log( html.getContent() ) ;
  
  SpreadsheetApp.getUi().showSidebar(html);
}

function include() 
{
  const filename = "client3" ;
  const toReturn = HtmlService.createHtmlOutputFromFile(filename).getContent().replaceAll("&lt;", "<").replaceAll("&gt;" , ">")  ;

  // Logger.log(toReturn); 

  return toReturn ; 
}