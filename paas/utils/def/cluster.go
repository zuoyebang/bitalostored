// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package def

const (
	BudgetOcr     = "ocr"
	BudgetYonghu  = "yonghu"
	BudgetLive    = "live"
	BudgetAb      = "ab"
	BudgetBigdata = "bigdata"
	BudgetTf      = "toufang"
	BudgetLx      = "laxin"
	BudgetPeople  = "people"
	BudgetAigc    = "aigc"
	BudgetStored  = "stored"
)

const (
	IdcTencent = "tencent"
	IdcTxcloud = "txcloud"
	IdcTxsh    = "txsh"
	IdcTxgz    = "txgz"
	IdcAli     = "ali"
	IdcBaidu   = "baidu"
	IdcALL     = "ALL"
)

const (
	ClusterGroupOcr    = "ocr"
	ClusterGroupYonghu = "yonghu"
	ClusterGroupLive   = "live"
	ClusterGroupAb     = "ab"
	ClusterGroupBd     = "bigdata"
	ClusterGroupTf     = "toufang"
	ClusterGroupLx     = "laxin"
	ClusterGroupPeople = "people"
	ClusterGroupAigc   = "aigc"
)

func TransformDbClusterName(clusterName string) string {
	if clusterName == "ocr-search-page" {
		clusterName = "our-search-page"
	}
	if clusterName == "ocr-search-inv" {
		clusterName = "our-search-inv"
	}
	return clusterName
}

func TransformOutputClusterName(clusterName string) string {
	if clusterName == "our-search-page" {
		clusterName = "ocr-search-page"
	}
	if clusterName == "our-search-inv" {
		clusterName = "ocr-search-inv"
	}
	return clusterName
}
