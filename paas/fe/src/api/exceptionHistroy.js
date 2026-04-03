import io from "@/utils/io";
const baseUrl = '/bitalospaas';

export function getExceptionHistroy(params) {
  return io.get(baseUrl + '/historyexception/list',params).then((data) => {
    return data.data;
  });
}
