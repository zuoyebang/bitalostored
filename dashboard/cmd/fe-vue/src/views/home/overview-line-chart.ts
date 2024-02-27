import Vue from 'vue'
import LineChart from './line-chart'

export default Vue.extend({
  props: ['data'],
  components: {LineChart},
  render(h) {
    return h('LineChart', {props: {chartData: this.chartData, height: 300}})
  },
  computed: {
    chartData() {
      const {data} = this
      return {
        labels: data.map((i) => i.label),
        datasets: [
          {
            label: 'cdm',
            borderColor: '#f87979',
            data: data.map((i) => i.cdm),
          },
          {
            label: 'asyncCal',
            borderColor: '#0495f8',
            data: data.map((i) => i.asyncCal),
          },
          {
            label: 'asyncDirect',
            borderColor: '#00f862',
            data: data.map((i) => i.asyncDirect),
          },
        ],
      }
    },
  },
})
