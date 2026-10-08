// Isolated, in-memory UI fixture. Never connects to a real gateway or database.
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../static');
const time = Date.now();
const agents = [
  { agent_id:'research', id:'research', name:'研发智能体', owner_id:'demo', brand_id:'code', status:'online', state:'running', conn_type:'stdio', launch_cmd:'node /demo/code-agent.mjs', capabilities:[], platform:{os:'linux',arch:'arm64'}, last_heartbeat:new Date().toISOString() },
  { agent_id:'docs', id:'docs', name:'文档智能体', owner_id:'demo', brand_id:'docs', status:'offline', state:'failed', error:'知识库服务暂时不可达，请检查服务地址。', conn_type:'http', launch_cmd:'http://127.0.0.1:9900', platform:{os:'linux',arch:'arm64'}, capabilities:[] }
];
const products = [
  { brand:'code', brand_id:'code', name:'代码审阅', description:'分析项目结构，检查变更并解释代码。', kind:'stdio', installed_version:'1.2.0', remote_version:'1.2.0-rc.1', has_instance:true, instance_from_install:true, platform_verified:true, remote_size:12000000 },
  { brand:'docs', brand_id:'docs', name:'文档整理', description:'将分散资料整理成可查阅的项目文档。', kind:'http', remote_version:'1.0.0', platform_verified:true, remote_size:6000000 },
  { brand:'knowledge', brand_id:'knowledge', name:'知识检索', description:'查询组织知识库，返回带来源的答案。', kind:'stdio', installed_version:'0.8.0', remote_version:'0.9.0', platform_verified:true, remote_size:14000000 },
  { brand:'windows-tool', brand_id:'windows-tool', name:'Windows 专用工具', description:'用于 Windows 环境的桌面自动化。', kind:'stdio', incompatible:true }
];
const users = [{id:'demo',name:'演示管理员',role:'admin',disabled:false,created_at:time,last_login_at:time},{id:'reviewer',name:'审阅人员',role:'user',disabled:false,created_at:time,last_login_at:time}];
const state = { configured:true, connected:true, gateway:'wss://demo.invalid:8443/ws/agent', gateway_base:'https://demo.invalid:8443', connector_id:'研发终端-07', hostname:'研发终端-07', config_path:'/demo/connector.json', products_dir:'/demo/products', platform:{os:'linux',arch:'arm64'}, active_tasks:1, pairing:{status:'idle'}, agents };
let failState = false, failCatalog = false, writes = 0;
const server = http.createServer(async (req,res) => {
  const url = new URL(req.url,'http://localhost');
  const json = (value, code=200) => {res.writeHead(code,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(value));};
  if(url.pathname==='/__fixture' && req.method==='POST') {
    let raw='';for await(const chunk of req)raw+=chunk;const control=JSON.parse(raw||'{}');
    if(control.failState!==undefined)failState=control.failState;if(control.failCatalog!==undefined)failCatalog=control.failCatalog;
    if(control.configured!==undefined)state.configured=control.configured;
    return json({writes,failState,failCatalog});
  }
  if(url.pathname==='/auth/config')return json({});
  if(url.pathname==='/auth/me')return json({user:users[0]});
  if(url.pathname==='/auth/login')return json({token:'ui-fixture-token',user:users[0]});
  if(url.pathname==='/api/state')return failState?json({error:'fixture outage'},503):json(state);
  if(url.pathname==='/api/product-catalog')return failCatalog?json({error:'fixture outage'},503):json({products});
  if(url.pathname==='/api/activities')return json({activities:[{id:'one',title:'更新知识检索',stage:'下载与校验安装包',status:'running',updated_at:time},{id:'two',title:'启用文档智能体',stage:'启用失败',status:'failed',error:'无法连接知识库服务',updated_at:time-60000},{id:'three',title:'连接组织',stage:'接入成功',status:'done',updated_at:time-120000}]});
  if(url.pathname==='/api/brands')return json({brands:[{id:'code',name:'代码审阅',conn_type:'stdio'}]});
  if(url.pathname.startsWith('/api/') && req.method!=='GET') {
    writes++; let raw='';for await(const part of req)raw+=part;
    if(url.pathname==='/api/settings/products-dir')state.products_dir=JSON.parse(raw).dir;
    setTimeout(()=>json({status:'ok',products_dir:state.products_dir,agent_id:'fixture-agent'}),700);return;
  }
  if(url.pathname==='/clients/catalog')return json({releases:['win-x64','win7-x64','linux-arm64','linux-x64-web'].map((target,i)=>({target,label:['Windows 10/11 · 64 位','Windows 7 · 64 位兼容版','麒麟 ARM64 · 解压版','Linux x64 · Web 管理版'][i],version:'1.1.2',file:'demo-'+target+(target.startsWith('win')?'.exe':'.tar.gz'),size:84000000,sha256:'a'.repeat(64),tested:false}))});
  if(url.pathname==='/products/catalog')return json({products:[]});
  const mapped={'/':'index.html','/client':'client.html','/admin':'admin.html','/clients':'downloads.html','/docs':'docs.html','/sw.js':'sw.js'};
  const name=mapped[url.pathname]||(url.pathname.startsWith('/static/')?url.pathname.slice(8):null);
  if(!name||name.includes('/')||name.includes('..')){res.writeHead(404);res.end();return;}
  try {const body=await fs.readFile(path.join(root,name));const types={'.html':'text/html; charset=utf-8','.js':'text/javascript','.css':'text/css','.png':'image/png','.svg':'image/svg+xml','.webmanifest':'application/manifest+json'};res.writeHead(200,{'Content-Type':types[path.extname(name)]||'application/octet-stream','Cache-Control':'no-store'});res.end(body);}catch{res.writeHead(404);res.end();}
});
new WebSocketServer({server}).on('connection',socket=>socket.on('message',raw=>{
  const msg=JSON.parse(raw), p=msg.params||{};let result={};
  if(msg.method==='admin.overview')result={users_total:2,agents_total:2,agents_online:1,users_connected:1,tasks_active:1};
  if(msg.method==='session.list')result={sessions:[{id:'s1',agent_id:'research',title:'项目代码审阅',created_at:time,updated_at:time,message_count:0}]};
  if(msg.method==='message.list')result={messages:[],total:0};
  if(msg.method==='group.list')result={groups:[]};
  if(msg.method==='user.list')result={users,total:users.length};
  if(msg.method==='agent.list')result={agents,total:agents.length};
  if(msg.method==='brand.list')result={brands:[]};
  if(msg.method==='device_key.list')result={keys:[]};
  if(msg.method==='pairing.list')result={codes:[]};
  if(msg.method==='connector.pending_list')result={connectors:[]};
  if(msg.method==='connector.list')result={connectors:[]};
  if(msg.method==='pairing.create')result={id:'fixture-pair',code:'DEMO-PAIR',expires_at:time+600000};
  if(msg.id)socket.send(JSON.stringify({jsonrpc:'2.0',id:msg.id,result}));
  if(msg.method==='auth')socket.send(JSON.stringify({jsonrpc:'2.0',method:'admin.agentList',params:{agents}}));
}));
server.listen(Number(process.env.UI_PREVIEW_PORT||8766),'127.0.0.1',()=>console.log('Isolated UI fixture: http://127.0.0.1:8766/client (gateway demo / demo)'));
