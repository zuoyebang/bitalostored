import io from "@/utils/io";
const baseUrl = '/bitalospaas';

export function getOperationList(params) {
  return io.get(baseUrl + '/operation/list',params).then((data) => {
    return data.data;
  });
}
