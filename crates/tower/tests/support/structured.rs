pub const NATIVE: &str = r#"#!/usr/bin/env node
const fs=require('node:fs'),path=require('node:path'),readline=require('node:readline');
const harness=path.basename(process.argv[1]);let session='native-fixture';
if(harness==='claude'){const flag=process.argv.includes('--resume')?'--resume':'--session-id';session=process.argv[process.argv.indexOf(flag)+1];}
const send=v=>process.stdout.write(JSON.stringify(v)+'\n');
function done(cancel=false){
 if(harness==='claude'){send({type:'assistant',message:{content:[{type:'text',text:'Fixture answer'}]}});send({type:'result',subtype:cancel?'error_during_execution':'success',is_error:cancel,terminal_reason:cancel?'aborted_tools':'end_turn'});}
 else{send({event:'step_update',step_update:{conversation_id:session,step_index:2,state:'DONE',step_type:'agent_response',text_delta:'Fixture answer'}});send({event:'result',result:{conversation_id:session,status:'SUCCESS',response:'Fixture answer'}});}
}
function prompt(text){
 fs.appendFileSync(path.join(process.env.HOME,'dispatches'),harness+':'+text+'\n');
 if(text==='question'){
  const question={id:'decision',header:'Decision',question:'PRIVATE-FORM-MARKER',multiSelect:false,options:[{label:'yes',description:'Continue'},{label:'no',description:'Stop'}]};
  send({type:'control_request',request_id:'question',request:{subtype:'can_use_tool',tool_name:'AskUserQuestion',tool_use_id:'question-tool',input:{questions:[question]}}});
 }else if(text==='approve'){
  send({type:'control_request',request_id:'permission',request:{subtype:'can_use_tool',tool_name:'Bash',tool_use_id:'tool',input:{command:'PRIVATE-REQUEST-MARKER'}}});
 }else if(text!=='wait')done();
}
let initialized=false;
readline.createInterface({input:process.stdin}).on('line',line=>{
 const v=JSON.parse(line);
 if(harness==='claude'){
  if(v.type==='control_request'){send({type:'control_response',response:{subtype:'success',request_id:v.request_id,response:{}}});if(v.request.subtype==='interrupt')done(true);}
  else if(v.type==='user')prompt(v.message.content.map(b=>b.text).join(''));
  else if(v.type==='control_response'){if(v.response.request_id==='question')require('node:assert/strict').equal(v.response.response.updatedInput.answers['PRIVATE-FORM-MARKER'],'yes');done();}
 }else if(v.event==='user'){if(!initialized){send({event:'init',conversation_id:session});initialized=true;}prompt(v.message.content.map(b=>b.text).join(''));}
});
"#;
