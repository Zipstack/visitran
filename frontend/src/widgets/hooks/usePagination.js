import { useState, useCallback } from "react";

export const usePagination = ({
  defaultPageSize = 10,
  defaultPage = 1,
} = {}) => {
  const [currentPage, setCurrentPage] = useState(defaultPage);
  const [pageSize, setPageSize] = useState(defaultPageSize);
  const [totalCount, setTotalCount] = useState(0);

  const onPaginationChange = useCallback((newPage, newPageSize) => {
    setCurrentPage(newPage);
    setPageSize(newPageSize);
  }, []);

  return {
    currentPage,
    pageSize,
    totalCount,
    setCurrentPage,
    setTotalCount,
    setPageSize,
    onPaginationChange,
  };
};
