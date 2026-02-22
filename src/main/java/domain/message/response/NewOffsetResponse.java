package domain.message.response;

public record NewOffsetResponse(
        long nextOffset,
        long logStartOffset
) {
}
