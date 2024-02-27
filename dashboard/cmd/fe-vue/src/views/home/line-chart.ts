import Vue from 'vue'
import Chart from 'chart.js'
import {getColor} from '@/constant'

export default Vue.extend({
  // extends: Line,
  props: ['chartData'],
  data() {
    return {chart: null}
  },
  render(h) {
    return h('div', {style: 'height: 300px'}, [
      h('canvas', {ref: 'chart'}),
    ])
  },
  mounted(): void {
    this.chart = new Chart(
      this.$refs.chart,
      {
        type: 'line',
        data: {
          labels: [],
          datasets: [
            {
              label: 'cmd',
              borderColor: getColor(0),
              fill: false,
              data: [],
            }
          ],
        },
        options: {
          // onHover(event, elements) {
          //   if (elements.length > 0) {
          //     console.log(elements, this)
          //   }
          // },
          tooltips: {
            mode: 'x',
          },
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            xAxes: [{
              // gridLines: {
              //   disabled: false,
              //   color: 'grey',
              //   offsetGridLines: true,
              // },
            }],
            yAxes: [{
              // gridLines: {
              //   disabled: false,
              //   color: 'grey',
              // },
              ticks: {
                beginAtZero: true,
              },
            }],
          },
        },
      })
    // this.$refs.chart.renderChart(
    //   this.chartData,
    //   {
    //     responsive: true,
    //     maintainAspectRatio: false,
    //     scales: {
    //       yAxes: [{
    //         ticks: {
    //           beginAtZero: true,
    //         },
    //       }],
    //     },
    //   })
  },
  watch: {
    chartData: {
      // immediate: true,
      handler(val) {
        this.chart.data.datasets.forEach((d) => {
          switch (d.label) {
            case 'cmd':
              d.data.push(val['cmd'])
              break
          }
          if (d.data.length > 20) {
            d.data.shift()
          }
        })
        this.chart.data.labels.push(val.label)
        if (this.chart.data.labels.length > 20) {
          this.chart.data.labels.shift()
        }
        this.chart.update()
      },
    },
  },
})
