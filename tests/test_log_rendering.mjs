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

test('business estimate remains visibly separate from actual settlement', () => {
  const context=vm.createContext({window:{},document:{querySelector:()=>({})},AbortController});
  vm.runInContext(readFileSync(new URL('../dashboard/dashboard.mjs',import.meta.url),'utf8'),context);
  const run=code=>vm.runInContext(code,context);
  run(`logs={accounts:[],summary:{task_count:1,budget:{amount:null,unknown_count:1},settlement:{amount:null,unknown_count:1},budget_occupancy:{amount:'0.25',unknown_count:0,estimated_count:1}},items:[{created_at:'2026-10-16T00:00:00Z',task_id:'task',model_id:'H3',state:'completed',budget:{amount:null},settlement:{amount:null},business_estimate:{amount:'0.25'},budget_occupancy:{amount:'0.25',source:'business_estimate'}}]}`);
  const html=run('logView()');
  assert.match(html, /估算占用，非实际扣费/);
  assert.match(html, /估算费用 0.25 USD（非实扣）/);
  assert.match(html, /<td class="num">未知<\/td>/);
  assert.doesNotMatch(html, /已对账|已扣费/);
  run(`logs.items[0].settlement.amount='0.20';logs.items[0].budget_occupancy={amount:'0.20',source:'fal_billing_events'}`);
  assert.match(run('logView()'), /按实际结算/);
  assert.match(run('logView()'), /估算费用 0.25 USD（非实扣）/);
});
