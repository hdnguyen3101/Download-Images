
const { MongoClient } = require('mongodb');
const fs = require('fs');
const request = require('request');

const dbUrl = 'mongodb://root:%601234qwer%60@10.10.12.201:27017,10.10.12.202:27017,10.10.12.203:27017/?replicaSet=test&authSource=admin';

const baseUrl = './images';

function sleep(ms) {
  var start = new Date().getTime(), expire = start + ms;
  while (new Date().getTime() < expire) { }
  return;
}

var download = function (uri, filename, callback) {
  request.head(uri, function (err, res, body) {
    if (err) {
      console.log(err);
      callback();
      return;
    }
    console.log('status code: ', res.statusCode);
    if (res.statusCode >= 400) {
      callback();
      return;
    }
    console.log('content-type:', res.headers['content-type']);
    console.log('content-length:', res.headers['content-length']);

    request(uri).pipe(fs.createWriteStream(filename + '.' + res.headers['content-type'].split('/')[1])).on('close', callback);
  });
};

var recursiveDownload = function(data, index) {
  if (data.length == 0) return;
  const currItem = data.shift();
  const user_id = currItem.user_id;
  if (!fs.existsSync(`${baseUrl}/${user_id}`)) {
    index = 0;
    fs.mkdirSync(`${baseUrl}/${user_id}`);
  }
  download(currItem.uri, `${baseUrl}/${user_id}/${user_id}_${index}`, function() {
    console.log('done', currItem.uri);
    sleep(1500); // slepp 1.5 seconds
    recursiveDownload(data, index + 1);
  });
}


// Check folder exists
if (!fs.existsSync(baseUrl)) {
  fs.mkdirSync(baseUrl);
}


MongoClient.connect(dbUrl, function (err, db) {
  if (err) {
    console.log('Unable to connect to the mongoDB server. Error:', err);
  } else {
    //HURRAY!! We are connected. :)
    console.log('Connection established to', dbUrl);


    db.db('test-api-halo').collection('m200').aggregate([
      {
        $match: {
          mv206: /^(?:http(s)?:\/\/)?[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!\$&'\(\)\*\+,;=.]+$/gm
        }
      },
      {
        $sort: { pn100: 1}
      },
      {
        $project: {
          user_id: "$pn100",
          uri: '$mv206'
        }
      },
      {
        $match: {
          user_id: {$ne: null}
        }
      }
    ]).toArray(function (err, data) {
      if (err) throw err;
      recursiveDownload(data);
      //Close connection
      db.close();
    });

  }
});
