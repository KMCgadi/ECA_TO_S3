package com.s3.eca2.domain.toastHistory;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;

@Service
public class ToastHistoryService {

    private final ToastHistoryRepository toastHistoryRepository;

    public ToastHistoryService(ToastHistoryRepository toastHistoryRepository){
        this.toastHistoryRepository = toastHistoryRepository;
    }

    public ToastHistory find(long entityId) {
        return toastHistoryRepository.findById(entityId)
                .orElseThrow(() -> new EntityNotFoundException("ToastHistory not found for id :" + entityId));
    }

    public Page<ToastHistory> findToastHistoryByDate(Date start, Date end, Pageable pageable) {
        return toastHistoryRepository.findByRegDateBetweenOrModDateBetween(start, end, pageable);
    }
}
