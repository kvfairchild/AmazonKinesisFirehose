// Variable for the visualization instance
var plantExplorer;

// Start application by loading the data

$.getJSON("data/plants.json", createVis);

function createVis(data) {

    plantsData = data;

  plantExplorer = new plantExplorer("plant-explorer", data, [37.0902, -95.7129]);

}