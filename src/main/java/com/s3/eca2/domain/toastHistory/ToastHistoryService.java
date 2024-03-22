package com.s3.eca2.domain.toastHistory;

import com.s3.eca2.domain.ticketRelation.TicketRelation;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;
import java.util.List;

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

    public List<ToastHistory> findToastHistoryByDate(Date start, Date end) {
        return toastHistoryRepository.findByRegDateBetweenOrModDateBetween(start, end);
    }
}
