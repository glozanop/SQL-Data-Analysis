//-- --------------------------
//-- Question 1
//-- --------------------------
db = db.getSiblingDB("global_cities");
db.countries_states_cities.findOne();

db.countries_states_cities.aggregate([
    { $group: { _id: { region: "$region", subregion: "$subregion" }, numberOfCountries: { $sum: 1 } }},
    { $match: { numberOfCountries: { $gte: 10 } }},
    { $sort: { numberOfCountries: -1 }},
    { $project: { _id: 0, region: "$_id.region", "number of countries": { $toInt: "$numberOfCountries" }, subregion: "$_id.subregion" }}
]);
// Which subregions have 15 or 16 countries?
// Subregions that have 15 or 16 countires = Northern Europe, South America.



//-- --------------------------
//-- Question 2
//-- --------------------------
db = db.getSiblingDB("global_cities");
db.countries_states_cities.findOne();

//Part 1 
db.countries_states_cities.aggregate([
    { $unwind: "$states" },
    { $unwind: "$states.cities" },
    { $match: { "states.cities.name": "Rochester" }},
    { $project: { 
        _id: 0, country: "$name", state: "$states.name", city: "$states.cities.name", 
        latitude: "$states.cities.latitude", longitude: "$states.cities.longitude" }}
]);

//Part 2; Rochesters in the US
db.countries_states_cities.aggregate([
    { $unwind: "$states" },
    { $unwind: "$states.cities" },
    { $match: { "states.cities.name": "Rochester", "name": "United States" }},
    { $group: { _id: "$states.name" }},
    { $count: "NumberOfStatesWithRochester" }
]);
//How many states in the United States contain a city named "Rochester"?  Which of those cities (Rochester's in the United States) is located furthest north?
//There are 10 states in the United States with a city Named Rochester
//The Rochester in Washington is the city that is furthest north with latitude of 46.82177000.



//-- --------------------------
//-- Question 3
//-- --------------------------
db = db.getSiblingDB("global_cities");
db.countries_states_cities.findOne();

db.countries_states_cities.aggregate([
    { $match: { currency: "USD" }},
    { $unwind: "$states" },
    { $unwind: "$states.cities" },
    { $group: { _id: { country: "$name", currency: "$currency" }, totalCities: { $sum: 1 }}},
    { $sort: { totalCities: -1 }},
    { $project: { _id: 0, country: "$_id.country", currency: "$_id.currency", totalCities: 1 }}
]);
//The United States has the most cities listed in this result set. Which country has the second most cities listed? How many cities are listed for that country?
//The country with the second most cities listed is Ecuador with 115 cities.



//-- --------------------------
//-- Question 4
//-- --------------------------
db = db.getSiblingDB("yelp");
db.businesses.findOne();

db.businesses.aggregate([
    { $match: { state: "CA", "attributes.OutdoorSeating": "True", categories: { $in: ["Restaurants"] } } },
    { $group: { _id: "$city", numberOfRestaurantsWithOutdoorSeating: { $sum: 1 } } },
    { $match: { numberOfRestaurantsWithOutdoorSeating: { $gte: 10 } } },
    { $sort: { numberOfRestaurantsWithOutdoorSeating: -1 } },
    { $project: { _id: 0, "Number of restaurants with outdoor seating": {$toInt: "$numberOfRestaurantsWithOutdoorSeating"}, city: "$_id",} }
]);
//What common geographic trait do you notice about cities in California that have more than ten restaurants offering outdoor seating?
//The cities in California that have more than ten restaurants with outdoor seating all appear to be coastal cities with beaches.



//-- --------------------------
//-- Question 5
//-- --------------------------
db = db.getSiblingDB("yelp");
db.businesses.find();

db.businesses.aggregate([
    {$match: {city: "Philadelphia", state: "PA", categories: { $all: ["Restaurants", "Dim Sum"] },"attributes.GoodForKids": "True",
        "attributes.WheelchairAccessible": "True", stars: { $gte: 4.0 },review_count: { $gte: 250 }}},
    {$project: {"Restaurant name": "$name", "Full address": { $concat: ["$address", ", ", "$city", ", ", "$state", ", ", "$postal_code"] },
        "Star rating": {$toInt: "$stars"},_id: 0}}
]);
//Based on the results returned, which zip code in Philadelphia is most likely to contain Philadelphia's "Chinatown" neighborhood?
//Based on these Results, the 19107 zip code is most likely to contain the Chinatown neighborhood.



//-- --------------------------
//-- Question 6
//-- --------------------------
db = db.getSiblingDB("yelp");
db.businesses.find();

db.businesses.aggregate([
    { $match: { categories: { $all: ["Yoga", "Meditation Centers"] } } },
    { $group: { _id: "$state", "Number of Yoga and Meditation Centers": { $sum: 1 } } },
    { $sort: { "Number of Yoga and Meditation Centers": -1 } },
    { $limit: 5},
    { $project: { _id: 0, "Number of Yoga and Meditation Centers": {$toInt: "$Number of Yoga and Meditation Centers"}, State: "$_id" } }
]);
//Based on these results, do there appear to be more Yoga and Meditation Centers in Florida or California?
// Based on the results, there are more yoga and meditation centers in Florida.



//-- --------------------------
//-- Question 7
//-- --------------------------
db = db.getSiblingDB("yelp");
db.businesses.find();

db.users.aggregate([
  { $match: { name: { $in: ["Jennifer", "Jenny"] } } },
  { $group: {_id: "$name", totalUsers: { $sum: 1 }, totalReviewsWritten: { $sum: "$review_count" }, avgReviewsWritten: { $avg: "$review_count" }, 
      totalUsefulVotes: { $sum: "$useful" }, totalFunnyVotes: { $sum: "$funny" }, totalCoolVotes: { $sum: "$cool" }}},
  { $project: {_id: 1, totalUsers: 1, totalReviewsWritten: 1, avgReviewsWritten: 1, totalVotes: { useful: "$totalUsefulVotes", 
      funny: "$totalFunnyVotes", cool: "$totalCoolVotes" }}},
  { $sort: { totalReviewsWritten: -1 } }
]);
//The total number of users in the dataset named "Jennifer" and "Jenny".
////The total number of reviews that reviewers with each of these names have written.
//On average, do users named Jennifer or Jenny write more reviews?
//Are Jennifers or Jenny's more likely to vote other people's reviews useful, cool, or funny?
//provide a brief, clear, and concise 1-2 sentence English language statement that answers the questions above, using the data from your query results to support your answers.

//Jennifers wrote a total of 354,753 reviews with an average of 26.72 reviews per user and Jennies wrote 99,898 reviews with a average of 34.21 per user, higher than Jennifers.
//Jennifers are more likley to vote other reviews as useful and cool while Jennys are more likely to vote a review as funny.



//-- --------------------------
//-- Question 8
//-- --------------------------
db = db.getSiblingDB("sample_airbnb");
db.listingsAndReviews.findOne();

db.listingsAndReviews.aggregate([
  { $match: { "address.market": { $in: ["Barcelona", "Porto", "Sydney", "New York", "Montreal", "Istanbul"] } }},
  { $group: { 
      _id: "$address.market",
      "Highest nightly price": { $max: { $toDouble: "$price" } },
      "Average nightly price": { $avg: { $toDouble: "$price" } },
      "Lowest nightly price": { $min: { $toDouble: "$price" } },
      "Average cleaning fee": { $avg: { $toDouble: "$cleaning_fee" } }
  }},
  { $project: { 
      Market: "$_id", 
      "Highest nightly price": {$round: ["$Average nightly price", 2]}, 
      "Lowest nightly price": {$round: ["$Average nightly price", 2]}, 
      "Average nightly price": {$round: ["$Average nightly price", 2]}, 
      "Average cleaning fee": {$round: ["$Average cleaning fee", 2]}, 
      _id: 0 }},
  { $sort: { Market: 1 }}
]);
//Based purely on the results of this query, which of these markets seems to be "the most affordable"? Why?
// Based on these resluts, the market that seems most affordable is Porto because it has the lowest average nightly price and lwoest average cleaning fee.



//-- --------------------------
//-- Question 9
//-- --------------------------
db = db.getSiblingDB("sample_airbnb");
db.listingsAndReviews.findOne();

db.listingsAndReviews.aggregate([
  { $match: { "address.market": "Porto" }},
  { $group: {_id: "$host.host_id", "Host name": { $first: "$host.host_name" }, "Host location": { $first: "$host.host_location" },
      "Number of listings in Porto": { $sum: 1 }, "Average nightly price": { $avg: { $toDouble: "$price" }}
  }},
  { $sort: { "Number of listings in Porto": -1 }},
  { $limit: 3},
  { $project: {
      "Host name": 1, 
      "Host location": 1, 
      "Average nightly price": { $round: ["$Average nightly price", 2] }, 
      "Number of listings in Porto": {$toInt: "$Number of listings in Porto"},
      "Host ID": "$_id", 
      _id: 0 }}
]);
//Based on the results of this query, does the Airbnb rental market in Porto appear to be dominated by a small number of large rental companies, 
//or does it appear to mostly consist of landlords with a small number of rentals listed?

//Based on these result, the Airbnb Porto market appears to be dominated by a small number of large rental companies, as it is likely the case that Liiiving, Feels Like Home, 
//and YourOpo are companies and not people.



//-- --------------------------
//-- Question 10
//-- --------------------------
db = db.getSiblingDB("sample_airbnb");
db.listingsAndReviews.findOne();

db.listingsAndReviews.aggregate([
  { $match: { "address.market": "Montreal" } },
  { $unwind: "$reviews" },
  { $group: {_id: "$reviews.reviewer_id", "Reviewer name": { $first: "$reviews.reviewer_name" },"Number of reviews": { $sum: 1}}},
  { $match: { "Number of reviews": { $gte: 5}}},
  { $sort: { "Number of reviews": -1 } },
  { $project: {"Reviewer name": 1, "Number of reviews": {$toInt: "$Number of reviews"}, "Reviewer ID": "$_id", _id: 0}}
]);
//How many reviewers meet these criteria?
//In total, there are 6 people in Montral that have written five or more reviews.



//-- --------------------------
//-- Question 11
//-- --------------------------
db = db.getSiblingDB("nba");
db.games.findOne();

db.games.aggregate([
  { $match: { "box.players.player": {$in: ["Kobe Bryant", "Tim Duncan"]} } },
  { $unwind: "$box" },
  { $unwind: "$box.players" },
  { $match: {"box.players.player": {$in: ["Kobe Bryant", "Tim Duncan"]} } },
  { $group: {
      "_id": "$box.players.player",
      "NumOfGames":  { $sum: 1 }}}
]);

//Kobe Bryant played 1248 games, and Tim Duncan played 1188 games. 
//As a result, Kobe Bryant played more NBA games during the dates covered by this dataset.



//-- --------------------------
//-- Question 12
//-- --------------------------
db = db.getSiblingDB("nba");
db.games.findOne();

//The aggregation pipeline that lists all games played during the 2003-2004 NBA season (July 1, 2003 - June 30, 2004) 
//in which the losing team scored less than 70 points.
db.games.aggregate([
  {$match: {
      "date": { $gte: ISODate("2003-07-01T00:00:00Z"), $lte: ISODate("2004-06-30T23:59:59Z") },
      "teams": { $elemMatch: { "won": 0, "score": { $lt: 70 } } }}
  },
  {$project: {"_id": 0, "date": 1, "teams.name": 1, "finalScore": "$teams.score"}}
]);

//Find the total number of total games played in a typical NBA season to use as a baseline to answer this question.
db.games.aggregate([
  {$match: {"date": { $gte: ISODate("2003-07-01T00:00:00Z"), $lte: ISODate("2004-06-30T23:59:59Z") } } },
  {$group: {
            _id: null, 
            totalGames: { $sum: 1 } } }, 
  {$project: {"_id":0, "totalGames":1}}
  ]);
//We found that the total number of tatal games played during the 2003-2004 NBA season is 1189.
//We also know that there are 47 documents that meet the requirements.
// 47/1189 = 3.95%
//According to the query, it suggest that there is 3.95% teams score less than 70 points in an NBA basketball game.



//-- --------------------------
//-- Question 13
//-- --------------------------
db = db.getSiblingDB("nba");
db.games.findOne();

db.games.aggregate([
    {$match: {"date": {$gte: ISODate("1995-07-01T00:00:00Z"), $lte: ISODate("2005-06-30T23:59:59Z")}}},
    {$unwind: "$teams"},
    {$match: {"teams.home": true } },
    {$group: {
            _id: null, 
            totalGames: { $sum: 1 }, 
            homeWins: {$sum: {$cond: [{ $eq: ["$teams.won", 1] }, 1, 0]}}}
    },
    {$project: {
            _id: 0,
            totalGames: 1,
            homeWins: 1,
            percentage: {
                $multiply: [{ $divide: ["$homeWins", "$totalGames"] }, 100] } } }
]);
//The result indicates that NBA home teams won 60.37% of games between July 1, 1995, and June 30, 2005, 
//suggesting a moderate home-court advantage. 
//We classify this advantage as weak as playing home only improve the odds of winning by 10.37%.


//-- --------------------------
//-- Question 14
//-- --------------------------
db = db.getSiblingDB("nba");
db.games.findOne();

db.games.aggregate([
  {$match: {date: {$gte: ISODate("2000-01-01T00:00:00Z"),$lte: ISODate("2010-12-31T23:59:59Z")}}},
  {$unwind: "$box"},
  {$unwind: "$box.team"},
  {$project: {team_steals: "$box.team.stl", team_won: "$box.won"} },
  {$group: {
      _id: "$_id",
      teams: {$push: {steals: "$team_steals", won: "$team_won"} } }},
  {$project: {
      team1: { $arrayElemAt: ["$teams", 0] },
      team2: { $arrayElemAt: ["$teams", 1] } } },
  {$project: {higher_steals_won: {
      $cond: [
          { $gt: ["$team1.steals", "$team2.steals"] },
          { $eq: ["$team1.won", 1] },
          { $eq: ["$team2.won", 1] } ] } } },
  {$group: {
      _id: null,
      totalGames: { $sum: 1 },
      higherStealsWins: {$sum: {$cond: ["$higher_steals_won", 1, 0] } } } },
  {$project: {
      _id: 0,
      totalGames: 1,
      higherStealsWins: 1,
      likelihood: {$multiply: [{ $divide: ["$higherStealsWins", "$totalGames"] }, 100] } } }
]);
//Across all the games in the dataset, we found that there is 53.50% 
//that a team with the higher number of steals in a given game won that game. 







