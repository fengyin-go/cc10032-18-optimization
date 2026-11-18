import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'
import SamplingLabView from '@/views/SamplingLabView.vue'
import DebugWorkbenchView from '@/views/DebugWorkbenchView.vue'
import ThresholdReviewView from '@/views/ThresholdReviewView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/sampling-lab', name: 'sampling-lab', component: SamplingLabView },
    { path: '/debug-workbench', name: 'debug-workbench', component: DebugWorkbenchView },
    { path: '/threshold-review', name: 'threshold-review', component: ThresholdReviewView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
