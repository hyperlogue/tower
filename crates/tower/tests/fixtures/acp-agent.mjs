import readline from 'node:readline';
const send = v => process.stdout.write(JSON.stringify({jsonrpc:'2.0',...v})+'\n');
let active;
readline.createInterface({input:process.stdin}).on('line',line=>{
  if(line.includes('"method":"vendor/slow"')){Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,50);process.stdout.write(line+'\n');return;}
  if(line.includes('"method":"vendor/exit"')){process.stdout.write(line+'\n',()=>process.exit(0));return;}
  if(line.includes('"method":"vendor/future"')){process.stdout.write(line+'\n');return;}
  const v=JSON.parse(line);
  if(v.method==='initialize')send({id:v.id,result:{protocolVersion:1,agentCapabilities:{loadSession:true,promptCapabilities:{image:true}},vendorCapabilities:{future:true}}});
  else if(v.method==='session/new'||v.method==='session/load')send({id:v.id,result:{sessionId:v.params.sessionId??'fixture-session'}});
  else if(v.method==='session/prompt'){
    active=v.id;
    if(v.params.prompt?.[0]?.text==='question')send({id:'agent-question',method:'elicitation/create',params:{sessionId:'fixture-session',mode:'form',message:'PRIVATE-FRAME-MARKER',requestedSchema:{$ref:'vendor:future-schema'},vendorField:{keep:true}}});
    else if(v.params.prompt?.[0]?.text!=='wait'){
      send({method:'session/update',params:{sessionId:'fixture-session',update:{sessionUpdate:'agent_message_chunk',content:{type:'text',text:'hello'},vendorField:{keep:true}}}});
      send({id:v.id,result:{stopReason:'end_turn'}});active=undefined;
    }
  }else if(v.method==='session/cancel'){
    if(active!==undefined)send({id:active,result:{stopReason:'cancelled'}});active=undefined;
  }else if(v.id==='agent-question'){
    send({id:active,result:{stopReason:'end_turn',responseSeen:v.result??v.error}});active=undefined;
  }else if(v.method)send({id:v.id,error:{code:-32601,message:'fixture method unavailable'}});
});
