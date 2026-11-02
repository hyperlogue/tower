pub const NATIVE: &str = r#"#!/usr/bin/env node
const fs=require('node:fs'),path=require('node:path'),readline=require('node:readline');
const harness=path.basename(process.argv[1]);let turn=0,session='native-fixture';
if(harness==='claude'){const flag=process.argv.includes('--resume')?'--resume':'--session-id';session=process.argv[process.argv.indexOf(flag)+1];}
const send=v=>process.stdout.write(JSON.stringify(v)+'\n');const reply=(id,result)=>send({jsonrpc:'2.0',id,result});
function done(cancel=false){
 if(harness==='codex'){send({method:'item/agentMessage/delta',params:{threadId:session,itemId:'item',delta:'Fixture answer'}});send({method:'turn/completed',params:{threadId:session,turn:{id:String(turn),status:cancel?'interrupted':'completed'}}});}
 else if(harness==='claude'){send({type:'assistant',message:{content:[{type:'text',text:'Fixture answer'}]}});send({type:'result',subtype:cancel?'error_during_execution':'success',is_error:cancel,terminal_reason:cancel?'aborted_tools':'end_turn'});}
 else{send({event:'step_update',step_update:{conversation_id:session,step_index:2,state:'DONE',step_type:'agent_response',text_delta:'Fixture answer'}});send({event:'result',result:{conversation_id:session,status:'SUCCESS',response:'Fixture answer'}});}
}
function prompt(text){
 fs.appendFileSync(path.join(process.env.HOME,'dispatches'),harness+':'+text+'\n');
 if(text==='approve-backlog'){
  for(let i=0;i<450;i++)send({method:'item/started',params:{threadId:session,turnId:String(turn),item:{id:'tool-'+i,type:'commandExecution',command:'fixture'}}});
  text='approve';
 }
 if(text==='question'){
  const question={id:'decision',header:'Decision',question:'PRIVATE-FORM-MARKER',multiSelect:false,options:[{label:'yes',description:'Continue'},{label:'no',description:'Stop'}]};
  if(harness==='codex')send({id:'question',method:'item/tool/requestUserInput',params:{threadId:session,turnId:String(turn),itemId:'tool',isBlocking:true,questions:[question]}});
  else send({type:'control_request',request_id:'question',request:{subtype:'can_use_tool',tool_name:'AskUserQuestion',tool_use_id:'question-tool',input:{questions:[question]}}});
 }else if(text==='url'){
  send({id:'url',method:'mcpServer/elicitation/request',params:{threadId:session,turnId:String(turn),serverName:'fixture',mode:'url',message:'Authorize',elicitationId:'url-id',url:'https://example.invalid/connect?nonce=PRIVATE-URL-MARKER'}});
 }else if(text==='approve'){
  if(harness==='codex')send({id:'permission',method:'item/commandExecution/requestApproval',params:{threadId:session,turnId:String(turn),itemId:'tool',command:'PRIVATE-REQUEST-MARKER'}});
  else send({type:'control_request',request_id:'permission',request:{subtype:'can_use_tool',tool_name:'Bash',tool_use_id:'tool',input:{command:'PRIVATE-REQUEST-MARKER'}}});
 }else if(text!=='wait')done();
}
let initialized=false;
readline.createInterface({input:process.stdin}).on('line',line=>{
 const v=JSON.parse(line),p=v.params??{};
 if(harness==='codex'){
  if(v.method==='initialize')reply(v.id,{userAgent:'fixture'});
  else if(['thread/start','thread/resume'].includes(v.method))reply(v.id,{thread:{id:session}});
  else if(v.method==='turn/start'){turn++;reply(v.id,{turn:{id:String(turn),status:'inProgress'}});send({method:'turn/started',params:{threadId:session,turn:{id:String(turn)}}});prompt(p.input.map(b=>b.text).join(''));}
  else if(v.method==='turn/interrupt'){reply(v.id,{});done(true);}
  else if(v.id==='permission'&&v.result)done();
  else if(v.id==='question'&&v.result){require('node:assert/strict').equal(v.result.answers.decision.answers[0],'yes');done();}
  else if(v.id==='url'&&v.result){require('node:assert/strict').equal(v.result.action,'accept');require('node:assert/strict').equal(v.result.content,undefined);done();}
 }else if(harness==='claude'){
  if(v.type==='control_request'){send({type:'control_response',response:{subtype:'success',request_id:v.request_id,response:{}}});if(v.request.subtype==='interrupt')done(true);}
  else if(v.type==='user')prompt(v.message.content.map(b=>b.text).join(''));
  else if(v.type==='control_response'){if(v.response.request_id==='question')require('node:assert/strict').equal(v.response.response.updatedInput.answers['PRIVATE-FORM-MARKER'],'yes');done();}
 }else if(v.event==='user'){if(!initialized){send({event:'init',conversation_id:session});initialized=true;}prompt(v.message.content.map(b=>b.text).join(''));}
});
"#;
