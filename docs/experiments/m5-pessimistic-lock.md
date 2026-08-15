# Pessimistic Lock Inventory Concurrency Experiment

## 문제

M4 실험에서 Transaction은 적용되어 있었지만 Inventory 차감은 다음 Read-Modify-Write 흐름이었다.

```text
READ Inventory
-> CHECK quantity
-> MODIFY inventory.quantity
-> SAVE inventory
```

M4 동일 상품 동시 주문 실험 결과, 50개 동시 주문을 5 round 실행했을 때 총 250건 주문이 모두 성공했지만 총 Lost Updates 224가 관찰되었다.

## 해결 방법

Inventory row 조회에 TypeORM `pessimistic_write` lock을 적용했다.

```ts
const inventory = await inventoryRepository.findOne({
  where: { productId: item.productId },
  lock: { mode: 'pessimistic_write' },
});
```

이 코드는 기존 `dataSource.transaction(async (manager) => ...)` 내부에서 `manager.getRepository(Inventory)`로 얻은 Repository를 통해 실행된다. Product, Order, OrderItem에는 Lock을 걸지 않았다.

## 동작 원리

동일 Inventory row를 수정하는 Transaction이 동시에 들어오면 먼저 Lock을 획득한 Transaction이 처리되는 동안 다른 Transaction은 해당 row의 write lock 해제를 기다린다.

현재 비즈니스 로직의 Read-Check-Modify-Save 구조는 유지하되, 같은 Inventory row를 동시에 읽고 같은 quantity 값을 기반으로 저장하는 상황을 막는다.

## 환경

- 테스트 날짜: 2026-08-15 18:23:26 KST
- Node version: v22.6.0
- MySQL Docker image: mysql:8.0
- MySQL version: 8.0.46
- 실행 환경: macOS Darwin 23.6.0 arm64
- 기준 commit SHA: f5b06c427962bf774d93be6c368f2a4ae4bb6864
- M5 변경 사항은 이 commit 위의 uncommitted 작업으로 측정했다.

## Test Scenario

M4와 동일한 조건으로 실행했다.

| 설정                 |  값 |
| -------------------- | --: |
| Initial stock        |  50 |
| Concurrency          |  50 |
| Quantity per request |   1 |
| Rounds               |   5 |

## Results

| Round | Product ID | Success | Failed | Expected Inventory | Actual Inventory | Lost Updates | Elapsed Time |
| ----- | ---------: | ------: | -----: | -----------------: | ---------------: | -----------: | -----------: |
| 1     |         16 |      50 |      0 |                  0 |                0 |            0 |        614ms |
| 2     |         17 |      50 |      0 |                  0 |                0 |            0 |        394ms |
| 3     |         18 |      50 |      0 |                  0 |                0 |            0 |        336ms |
| 4     |         19 |      50 |      0 |                  0 |                0 |            0 |        406ms |
| 5     |         20 |      50 |      0 |                  0 |                0 |            0 |        322ms |

## Status Distribution

| Status bucket | Count |
| ------------- | ----: |
| 2xx           |   250 |
| 400           |     0 |
| 404           |     0 |
| 409           |     0 |
| 5xx           |     0 |
| network/error |     0 |
| other         |     0 |

Summary:

| 항목                    |  값 |
| ----------------------- | --: |
| Total requests          | 250 |
| Total successful orders | 250 |
| Total failed orders     |   0 |
| Total lost updates      |   0 |

## M4 vs M5

| 항목                       |                                M4 |                                M5 |
| -------------------------- | --------------------------------: | --------------------------------: |
| Concurrency                |                                50 |                                50 |
| Rounds                     |                                 5 |                                 5 |
| Initial stock per round    |                                50 |                                50 |
| Quantity per request       |                                 1 |                                 1 |
| Total requests             |                               250 |                               250 |
| Total success              |                               250 |                               250 |
| Total failed               |                                 0 |                                 0 |
| Total lost updates         |                               224 |                                 0 |
| Actual inventory per round |                44, 45, 45, 45, 45 |                     0, 0, 0, 0, 0 |
| Elapsed time per round     | 373ms, 288ms, 653ms, 422ms, 272ms | 614ms, 394ms, 336ms, 406ms, 322ms |

## 재고 초과 주문 테스트

기본 50/50 실험 후 별도로 재고보다 주문이 많은 경우를 테스트했다.

| 항목                 |    값 |
| -------------------- | ----: |
| Initial stock        |    10 |
| Concurrent requests  |    50 |
| Quantity per request |     1 |
| Rounds               |     1 |
| Product ID           |    21 |
| Success              |    10 |
| Failed               |    40 |
| Failed status        |   409 |
| Expected inventory   |     0 |
| Actual inventory     |     0 |
| Lost updates         |     0 |
| Elapsed time         | 170ms |

Status distribution:

| Status bucket | Count |
| ------------- | ----: |
| 2xx           |    10 |
| 400           |     0 |
| 404           |     0 |
| 409           |    40 |
| 5xx           |     0 |
| network/error |     0 |
| other         |     0 |

최종 Inventory는 0으로 확인되었고 음수가 되지 않았다.

## Trade-off

이번 측정에서 M5는 Lost Update를 제거했다. M4 대비 elapsed time은 round별로 증가한 경우도 있고 감소한 경우도 있었다. 이 측정만으로 일반적인 성능 결론을 내리지는 않는다.

Pessimistic Lock은 동일 row에 대한 변경을 직렬화하므로 정합성을 위한 후보가 될 수 있지만, Lock 대기 때문에 동시 처리량이나 latency에 영향을 줄 가능성이 있다. 이 부분은 별도의 성능 측정이 필요하다.

## 결론

M4에서 재현된 Inventory Lost Update는 M5에서 동일 조건으로 재실험했을 때 재현되지 않았다. 총 Lost Updates는 224에서 0으로 감소했다.

이번 단계에서는 Pessimistic Write Lock만 적용했고, Optimistic Lock, VersionColumn, Atomic Conditional Update, Redis Lock, Retry, Queue, Isolation Level 변경은 구현하지 않았다.
