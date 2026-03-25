<template>
  <div class="action">
    <el-dialog
      title="upgrade"
      size="small"
      :before-close="close"
      :visible.sync="dialogAction"
    >
      <el-form
        class="create-machine"
        :model="actionObj"
        ref="actionForm"
        :rules="actionRules"
        label-width="150px"
      >
        <el-form-item label="operation:" prop="operation">
          <el-select v-model="actionObj.operation" placeholder="please select">
            <el-option
              v-for="(item, index) in operationList"
              :key="index"
              :label="item"
              :value="item"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="version:" prop="packageId">
          <el-select v-model.number="actionObj.packageId" placeholder="please select">
            <el-option
              v-for="(item, index) in packageList"
              :key="index"
              :label="item.version"
              :value="item.id"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="updateConfig:" prop="updateConfig">
          <el-select v-model="actionObj.updateConfig">
            <el-option
              v-for="(item, index) in options"
              :key="index"
              :label="item.label"
              :value="item.value"
            >
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="close()">Cancel</el-button>
        <el-button type="primary" @click="updateAction('actionForm')"
          >Confirm</el-button
        >
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { getPackageList } from "@/api/matrix";
import { updateAction, getOperationList } from "@/api/";

export default {
  data() {
    return {
      packageList: [],
      operationList: [],
      actionObj: {
        operation: "",
        packageId: "",
        updateConfig: "false",
      },
      actionRules: {
        packageTag: [
          {
            required: true,
            message: "please select packageTag",
            trigger: "change",
          },
        ],
        operation: [
          {
            required: true,
            message: "please select operation",
            trigger: "change",
          },
        ],
      },
      options: [
        {
          value: "true",
          label: "Yes",
        },
        {
          value: "false",
          label: "No",
        },
      ],
    };
  },
  props: {
    actionForm: {
      type: Object,
      required: true,
      default: () => ({}),
    },
    dialogAction: {
      type: Boolean,
      required: true,
      default: false,
    },
  },
  methods: {
    close() {
      this.actionForm.operation = this.actionObj.operation;
      this.actionForm.packageId = this.actionObj.packageId;
      this.$emit("changAction", false);
    },
    updateAction(formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          updateAction(this.actionObj)
            .then(() => {
              this.actionForm.operation = this.actionObj.operation;
              this.actionForm.packageId = this.actionObj.packageId;
              this.$emit("changAction", false);
              this.$emit("getActionList", false);
              this.$message.success("success");
            })
            .catch((err) => {
              this.$emit("changAction", false);
              console.log(err);
            });
        } else {
          this.$message.error("Please select or enter required items!");
          return false;
        }
      });
    },
    getOperationList() {
      const params = {
        serviceId: this.actionForm.serviceId,
      };
      getOperationList(params)
        .then((res) => {
          const operations = res.operations || [];
          this.operationList = operations.filter(function(item) {
            if ( item == "supervisor-stop" ){
              return false
            } else {
              return true
            }
          })
          if (this.operationList.length > 0) {
            if (!this.operationList.includes(this.actionObj.operation)) {
              this.actionObj.operation = this.operationList[0];
            }
          }
        })
        .catch((err) => {
          console.log(err);
        });
    },
    getPackageList() {
      const params = {
        serviceId: this.actionForm.serviceId,
        clusterId: this.$route.query.clusterId || this.actionObj.clusterId,
      };
      getPackageList(params)
        .then((res) => {
          this.packageList = res || [];
          if (this.packageList.length > 0) {
            if (
              this.packageList.findIndex(
                (obj) => obj.id === this.actionObj.packageId
              ) === -1
            ) {
              this.actionObj.packageId = "";
            }
          }
        })
        .catch((err) => {
          console.log(err);
        });
    },
  },
  watch: {
    dialogAction(val) {
      if (val) {
        this.actionObj = Object.assign(
          this.actionObj,
          JSON.parse(JSON.stringify(this.actionForm))
        );
        delete this.actionObj.packageTag;
        this.getPackageList();
        this.getOperationList();
      }
    },
  },
};
</script>

<style scoped lang="scss"></style>
