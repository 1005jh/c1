# Inventory Race Condition Experiment

## 목적

Transaction만 적용된 현재 주문 처리에서 동시 주문 시 재고 정합성이 깨지는지 확인한다.

## Baseline 구현

현재 재고 차감 흐름은 다음과 같다.

```text
READ Inventory
-> CHECK quantity
-> MODIFY inventory.quantity
-> SAVE inventory
```

주문 생성은 DB Transaction으로 묶여 있지만, Pessimistic Lock, Optimistic Lock, atomic conditional update, Redis lock은 적용하지 않았다.

## 환경

- 테스트 날짜: 2026-08-15 18:02:00 KST
- Node version: v22.6.0
- MySQL Docker image: mysql:8.0
- MySQL version: 8.0.46
- 실행 환경: macOS Darwin 23.6.0 arm64
- 테스트한 commit SHA: c457bb3d1b9e9a038cc404c5ea59c3afc5b2a986
- 참고: 실험 스크립트와 이 문서는 uncommitted M4 작업으로 추가했고, production 주문 로직은 변경하지 않았다.

## M3 Baseline 재검증

실험 전 다음 명령을 실행했다.

```text
npm test: 4 suites passed, 17 tests passed
npm run build: 성공
npm run migration:run: No migrations are pending
```

Rollback 재확인:

| 항목 | 값 |
| --- | --- |
| Product A inventory before | 10 |
| Product B inventory | 1 |
| 주문 요청 | A 2개 + B 999개 |
| 주문 결과 | 409 Conflict |
| Product A inventory after | 10 |

## Test Scenario

| 설정 | 값 |
| --- | --- |
| BASE_URL | http://localhost:3000 |
| Initial inventory | 50 |
| Concurrent requests | 50 |
| Quantity per request | 1 |
| Rounds | 5 |

각 round는 새 Product와 새 Inventory를 생성한 뒤 같은 Product에 대해 50개 주문을 동시에 요청했다.

## Results

| Round | Product ID | Initial Inventory | Requests | Successful Orders | Failed Orders | Expected Inventory | Actual Inventory | Lost Updates | Elapsed Time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 11 | 50 | 50 | 50 | 0 | 0 | 44 | 44 | 373ms |
| 2 | 12 | 50 | 50 | 50 | 0 | 0 | 45 | 45 | 288ms |
| 3 | 13 | 50 | 50 | 50 | 0 | 0 | 45 | 45 | 653ms |
| 4 | 14 | 50 | 50 | 50 | 0 | 0 | 45 | 45 | 422ms |
| 5 | 15 | 50 | 50 | 50 | 0 | 0 | 45 | 45 | 272ms |

Status distribution:

| Status bucket | Count |
| --- | ---: |
| 2xx | 250 |
| 400 | 0 |
| 404 | 0 |
| 409 | 0 |
| 5xx | 0 |
| network/error | 0 |
| other | 0 |

Summary:

| 항목 | 값 |
| --- | ---: |
| Total requests | 250 |
| Total successful orders | 250 |
| Total failed orders | 0 |
| Total lost updates | 224 |

## Expected vs Actual

각 round의 기대 재고는 다음 공식으로 계산했다.

```text
expectedInventory = initialStock - successfulOrders * orderQuantity
```

모든 round에서 `initialStock=50`, `successfulOrders=50`, `orderQuantity=1`이므로 기대 재고는 0이다. 실제 DB 재고는 44 또는 45로 남았다.

## 관찰 결과

Race Condition이 재현되었다. 모든 주문 요청은 2xx로 성공했지만, 성공 주문 수만큼 재고 감소가 최종 DB 값에 반영되지 않았다.

Round 1의 경우 성공 주문은 50개였으므로 기대 재고는 0이어야 한다. 그러나 실제 재고는 44였고, `lostUpdates=44`가 관찰되었다.

## 원인 분석

현재 구현은 각 주문 Transaction 안에서 Inventory를 읽고, 애플리케이션 메모리에서 quantity를 감소시킨 뒤 save한다.

```text
Transaction A       Transaction B

READ quantity=N     READ quantity=N
N-1                 N-1
SAVE N-1            SAVE N-1
```

두 주문이 모두 성공해도 최종 값에는 하나의 감소만 반영될 수 있다.

DB Transaction은 하나의 주문 내부의 원자성은 보장하지만, 동시에 실행되는 여러 Transaction 사이의 Lost Update까지 현재 구현에서 자동으로 방지하지 않는다.

## 다음 단계 후보

아직 구현하지 않는다. 다음 M 단계에서 비교 검토할 후보만 기록한다.

- Pessimistic Lock
- Optimistic Lock
- Atomic Conditional Update
- Redis Distributed Lock
