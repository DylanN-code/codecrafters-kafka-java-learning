package domain.message;

public record Request(
        Header header,
        RequestBody requestBody
) {
}
