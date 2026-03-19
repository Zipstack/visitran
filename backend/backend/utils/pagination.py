from typing import Any, Union

from django.core.paginator import EmptyPage, PageNotAnInteger, Paginator
from django.db.models import QuerySet


class CustomPaginator:
    """Reusable pagination utility that works with both Django views and DRF
    APIs."""

    def __init__(self, queryset: QuerySet[Any], limit: int = 30, page: int = 1) -> None:
        """Initialize the paginator.

        Args:
            queryset: Django queryset to paginate
            limit: Default items per page
            page: page number to make query
        """
        self.queryset = queryset
        self.page_size = limit
        self.page_number = page
        self.paginator = Paginator(queryset, self.page_size)

    def paginate(self) -> dict[str, Union[list[Any], None, Any]]:
        """Perform pagination and return paginated data.

        Returns:
            queryset: Contains paginated data
        """
        try:
            page = self.paginator.page(self.page_number)
        except PageNotAnInteger:
            page = self.paginator.page(1)
        except EmptyPage:
            page = self.paginator.page(self.paginator.num_pages)

        return {
            "page_items": list(page.object_list),  # The actual items for current page
            "current_page": page.number,
            "total_pages": self.paginator.num_pages,
            "total_items": self.paginator.count,
            "has_next": page.has_next(),
            "has_previous": page.has_previous(),
            "next_page": page.next_page_number() if page.has_next() else None,
            "previous_page": page.previous_page_number() if page.has_previous() else None,
        }
