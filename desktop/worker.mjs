import { runClient, stopLocalProducts } from './client.mjs';
let stopping = false;
function stop() {
  if (stopping) return;
  stopping = true;
  stopLocalProducts();
  if (process.listenerCount('SIGTERM')) process.emit('SIGTERM');
  else setTimeout(() => process.exit(0), 4500);
}
process.on('message', message => { if (message?.type === 'client.stop') stop(); });
process.on('disconnect', stop);
await runClient();
// Keep escalation timers alive long enough to stop stubborn agent descendants.
await new Promise(resolve => setTimeout(resolve, 4500));
process.exit(0);
