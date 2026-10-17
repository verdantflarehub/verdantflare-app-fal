import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';

test('logs distinguish unfinished fees, unknown final fees and confirmed zero', () => {
  const context=vm.createContext({window:{},document:{querySelector:()=>({})},AbortController});
  vm.runInContext(readFileSync(new URL('../dashboard/dashboard.mjs',import.meta.url),'utf8'),context);
  const run=code=>vm.runInContext(code,context);
  run(`logs={accounts:[],summary:{task_count:1,budget:{amount:'5',unit:'USD',unknown_count:0},settlement:{amount:null,unit:'USD',unknown_count:1}},items:[{created_at:'2026-10-17T00:00:00Z',task_id:'task',account_name:'Account',model_id:'model',state:'completed',budget:{amount:'5',unit:'USD'},settlement:{amount:null,unit:'USD'}}]}`);
  for (const state of ['completed','failed','rejected']) {
    run(`logs.items[0].state='${state}'`);
    assert.match(run('logView()'), /<td class="num">未知<\/td>/);
    assert.doesNotMatch(run('logView()'), /待结算/);
  }
  for (const state of ['queued','in_queue','in_progress','acceptance_unknown']) {
    run(`logs.items[0].state='${state}'`);
    assert.match(run('logView()'), /<td class="num">待结算<\/td>/);
  }
  run("logs.items[0].state='completed';logs.items[0].settlement.amount='0'");
  assert.match(run('logView()'), /<td class="num">0 USD<\/td>/);
  run("logs.items[0].settlement.amount='6'");
  const html=run('logView()');
  assert.match(html, /<td class="num">5 USD<\/td>/);
  assert.match(html, /<td class="num">6 USD<\/td>/);
});
