
/*
 *  plantExplorer - Object constructor function
 *  @param _parentElement   -- HTML element in which to draw the visualization
 *  @param _data            -- Array with all stations of the bike-sharing network
 */

plantExplorer = function(_parentElement, _data) {
  this.parentElement = _parentElement;
  this.data = _data;

  this.initVis();
}


/*
 *  Initialize station map
 */

plantExplorer.prototype.initVis = function() {
  var vis = this;

  vis.margin = { top: 40, right: 0, bottom: 60, left: 60 };

  vis.width = 800 - vis.margin.left - vis.margin.right,
      vis.height = 400 - vis.margin.top - vis.margin.bottom;

  // SVG drawing area
  vis.svg = d3.select("#" + vis.parentElement).append("svg")
      .attr("width", vis.width + vis.margin.left + vis.margin.right)
      .attr("height", vis.height + vis.margin.top + vis.margin.bottom)
      .append("g")
      .attr("transform", "translate(" + vis.margin.left + "," + vis.margin.top + ")");

  // Create map

  plantExplorer = L.map("plant-explorer").setView([37.0902, -95.7129], 4);

  L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
  }).addTo(plantExplorer);


  vis.wrangleData();
}


/*
 *  Data wrangling
 */

plantExplorer.prototype.wrangleData = function() {
  var vis = this;

  // Currently no data wrangling/filtering needed
  // vis.displayData = vis.data;

  // Update the visualization
  vis.updateVis();
}


/*
 *  The drawing function
 */

plantExplorer.prototype.updateVis = function() {

  // Add empty layer group for the markers

  powerPlants = L.layerGroup().addTo(plantExplorer);
  for (i = 0; i < plantsData.length; i++) {

    // Create popups

    var popupContent =  "<strong>" + plantsData[i].name + "</strong><br/>"
        popupContent += plantsData[i].city + ", " + plantsData[i].state;

    // Create a marker and bind a popup with a particular HTML content
    var marker = L.marker([plantsData[i].lat, plantsData[i].long])
        .bindPopup(popupContent)
        .addTo(plantExplorer);
  }

}
