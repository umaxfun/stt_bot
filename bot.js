const Telegraf = require('telegraf');
const fs = require('fs')
const fetch = require('node-fetch')
const util = require('util');
const exec = util.promisify(require('child_process').exec);
const MongoClient = require('mongodb').MongoClient;
const crypto = require('crypto');

const connectionString = process.env.STT_MONGO_CONNSTRING || 'mongodb://localhost:27017';
const db_name = process.env.STT_DB_NAME || 'stt'
const token = process.env.IAM_TOKEN || 'AQVN3pg1cg_oK3zS5u8ggJWjbCarvEJfup8O33LM'
const bot = new Telegraf(process.env.BOT_TOKEN || '980480569:AAHmxr9NihLGfm9Ga56Wk8OXzObHzgv6UH0')


async function saveUpdate(obj) {
  let client = await MongoClient.connect(connectionString, { useNewUrlParser: true })
  let db = client.db(db_name);
  let records = db.collection('records');
  await records.updateOne({_id: obj._id}, { $set: obj}, {upsert: true})
}

async function getCollection() {
  let client = await MongoClient.connect(connectionString, { useNewUrlParser: true })
  let db = client.db(db_name);
  let records = db.collection('records');
  return records;
}

function getHash(filename, algorithm = 'sha1') {
  return new Promise((resolve, reject) => {
    // Algorithm depends on availability of OpenSSL on platform
    // Another algorithms: 'sha1', 'md5', 'sha256', 'sha512' ...
    let shasum = crypto.createHash(algorithm);
    try {
      let s = fs.ReadStream(filename)
      s.on('data', function (data) {
        shasum.update(data)
      })
      // making digest
      s.on('end', function () {
        const hash = shasum.digest('hex')
        return resolve(hash);
      })
    } catch (error) {
      return reject('calc fail');
    }
  });
}

async function checkStt(){
  //console.log('checkStt');
  const coll = await getCollection();
  const query = { "status": "stt scheduled" };
  const count = await coll.countDocuments(query);
  if (count === 0) {
    //console.log(`No docs in queue`)
    return;
  } else {
    console.log(`${count} docs in queue`)
  }
  
  const results = await coll.find(query);

  while (await results.hasNext()) {
    const msg = await results.next();
    console.log('Id:', msg.ticket.id)
    const rawResult = await fetch(`https://operation.api.cloud.yandex.net/operations/${msg.ticket.id}`, {
      headers: {
        'Authorization': `Api-Key ${token}`
      }
    });

    result = await rawResult.json();
    console.log(result);

    if(result.done === true) {
      msg.stt_result = result;
      msg.status = 'stt complete';
      await saveUpdate(msg);
    }
  }
}

async function checkPosted() {
  // console.log('checkPosted');
  const coll = await getCollection();
  const query = { "status": "stt complete" };
  const count = await coll.countDocuments(query);
  if (count === 0) {
    //console.log(`No docs in queue`)
    return;
  } else {
    console.log(`${count} docs in queue`)
  }
  
  const results = await coll.find(query);

  while (await results.hasNext()) {
    const msg = await results.next();
    // console.log(msg);
    const src_audio = msg.message.audio || msg.message.voice;
    const parts = [];
    msg.stt_result.response.chunks.forEach(c => {
      if (c.channelTag == 1) {
        parts.push(c.alternatives[0].text)
      }
    })

    const msgTemplate = 
// message templpate
/******************************************************************/
`
${src_audio.title || "Заметка"} - ${src_audio.duration} сек.

${parts.join(' ')}
`;
/******************************************************************/
    console.log(msgTemplate);
    const what_is_sent = await bot.telegram.sendMessage(msg.message.chat.id, msgTemplate);
    console.log(what_is_sent);
    msg.status = 'result sent';
    msg.sentmsg = what_is_sent;
    await saveUpdate(msg);  
  }
  
}

async function cron(){
  //console.log('Cron');
  await checkStt();
  await checkPosted();
  process.exit(0);
}

if (process.argv[2] === 'cron') {
  cron();

} else {

  bot.on(['voice','audio'], async (ctx) => {
    // ctx.reply('yo!');
    const upd = {
      _id: ctx.update.update_id,
      status: 'not started',
      ...ctx.update
    }
    console.log(upd);
    await saveUpdate(upd);
    const afile = ctx.update.message.voice || ctx.update.message.audio;
    const link = await ctx.tg.getFileLink(afile);
    const res = await fetch(link);
    const fname = `./${ctx.update.update_id}`;
    const dest = fs.createWriteStream(`${fname}.ogg`);
    res.body.pipe(dest);

    upd.hash = await getHash(`${fname}.ogg`);
    upd.status = 'file received';
    await saveUpdate(upd);

    await exec(`ffmpeg -i ${fname}.ogg -f wav - | opusenc --bitrate 256 - ${fname}.opus`);
    fs.unlinkSync(`${fname}.ogg`);
    
    upd.status = 'file converted';
    await saveUpdate(upd);

    if (afile.duration < 25) { // используем метод для коротких аудио
      console.log('short file')
      const stream = fs.createReadStream(`${fname}.opus`)
      const result = await (async () => {
        const rawResponse = await fetch('https://stt.api.cloud.yandex.net/speech/v1/stt:recognize', {
          method: 'POST',
          headers: {
            'Authorization': `Api-Key ${token}`,
            'Transfer-Encoding': 'chunked',
          },
          body: stream
        });
        const content = await rawResponse.json();
        return content;
      })();
      console.log(result);
      fs.unlinkSync(`${fname}.opus`);

      if (result.error_code !== undefined) {
        const sentmsg = await ctx.reply(`Что-то пошло не так у яндекса, попробуйте позже отфорвардить мне ту же запись\n ${result.error_code} ${result.error_message}`)
        upd.status = 'short file error';
        upd.error = result;
        upd.sentmsg = sentmsg;
        await saveUpdate(upd);
        return;
      }

      upd.stt_result = result;

      const msgTemplate = 
      // message templpate
      /******************************************************************/
`
${afile.title || "Заметка"} - ${afile.duration} сек.

${result.result}
`;
      /******************************************************************/

      const sentmsg = await ctx.reply(msgTemplate);
      console.log(sentmsg);
      
      upd.status = 'short file complete';
      upd.sentmsg = sentmsg;
      await saveUpdate(upd);

      return;
    }

    await exec(`aws --endpoint-url=https://storage.yandexcloud.net s3 cp ./${fname}.opus s3://umaxspeech/`);
    fs.unlinkSync(`${fname}.opus`);

    upd.status = 'file uploaded to cloud';
    await saveUpdate(upd);

    const ticket = await (async () => {
      const rawResponse = await fetch('https://transcribe.api.cloud.yandex.net/speech/stt/v2/longRunningRecognize', {
        method: 'POST',
        headers: {
          'Authorization': `Api-Key ${token}`,
        },
        body: JSON.stringify({
          "config": {
              "specification": {
                  "languageCode": "ru-RU"
              }
          },
          "audio": {
              "uri": `https://storage.yandexcloud.net/umaxspeech/${ctx.update.update_id}.opus`
          }
        })
      });
      const content = await rawResponse.json();
      return content;
    })();

    console.log(ticket);
    upd.status = 'stt scheduled';
    upd.ticket = ticket;
    await saveUpdate(upd);

  })
  
  bot.hears('hi', (ctx) => ctx.reply('Hey there'))
  bot.launch();
}