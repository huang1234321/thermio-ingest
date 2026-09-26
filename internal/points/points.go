// Package points 负责 point/gateway 配置缓存（ingest.md §6）：PG 只读 point、gateway
// 两表（thermio_ingest 旁路只读角色，ddl.md §5.1）；增量刷新 30s + 全量重对齐 1h +
// 刷新失败保旧缓存继续服务。缓存结构与刷新循环随 IMPL-5 落地。
package points
